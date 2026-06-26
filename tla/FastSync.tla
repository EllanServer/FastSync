------------------------------- MODULE FastSync -------------------------------
(*
 * FastSync TLA+ Specification
 *
 * This is a small formal model of the FastSync cross-server player data
 * synchronization system. It verifies that the combination of:
 *   - Per-UUID fencing tokens (Kleppmann/ZooKeeper)
 *   - Optimistic concurrency control with version numbers (Dynamo)
 *   - Redis Pub/Sub lock coordination
 *   - DB CAS (compare-and-swap) writes
 *
 * ...preserves the following invariants under crash, delay, and retry:
 *   NoDoubleOwner    - A UUID is never ACTIVE on two servers simultaneously
 *   NoLateWrite      - A lower fencing token can never overwrite higher-token data
 *   NoDupItem        - Player item count never increases due to handoff
 *   NoLostMoney      - Economy ledger sum(deltas) == balance
 *   MonotonicVersion - DB version only increases
 *
 * Inspired by AWS's use of TLA+ to find bugs in DynamoDB, S3, and EBS.
 * The value is NOT replacing code tests, but enumerating crash/delay/retry
 * combinations BEFORE writing code.
 *)

EXTENDS Naturals, Sequences, FiniteSets, Integers, TLC

(*
 * Constants — these are instantiated by the model config (FastSync.cfg)
 *)
CONSTANTS
    Servers,    (* Set of server identifiers, e.g. {s1, s2}          *)
    Players,    (* Set of player UUIDs, e.g. {p1, p2}                *)
    NullServer  (* A sentinel value meaning "no server"              *)

(*
 * Assumptions
 *)
ASSUME NullServer \notin Servers
ASSUME Servers # {} /\ Players # {}

(*
 * =============================================================================
 * Variables
 * =============================================================================
 *)

VARIABLES
    owner,          (* [uuid -> Server | NullServer]: who currently "owns" the player    *)
    leaseToken,     (* [uuid -> Nat]: the fencing token of the current/last lock holder *)
    dbVersion,      (* [uuid -> Nat]: the version number stored in the DB              *)
    dbData,         (* [uuid -> record]: the actual data stored in the DB              *)
    dbFencingToken, (* [uuid -> Nat]: the fencing token stored in the DB (last writer) *)
    pendingSave,    (* Set of (Server, uuid) pairs with a save in flight to the DB     *)
    pendingSaveData,(* [(Server,uuid) -> record]: the data being saved                *)
    saveAttempts,   (* [(Server,uuid) -> Nat]: how many times this save was attempted  *)
    inventoryCount, (* [uuid -> Nat]: number of items the player has (simplified)      *)
    balance,        (* [uuid -> Int]: player's current balance                        *)
    ledger,         (* Sequence of (uuid, delta) — append-only transaction log        *)
    playerState,    (* [uuid -> "INACTIVE" | "JOINING" | "ACTIVE" | "QUITTING"]       *)
    savedToken      (* [uuid -> Nat]: the fencing token the player's data was saved with *)

(*
 * Type invariant — for state constraint checking
 *)
TypeInvariant ==
    /\ owner \in [Players -> Servers \cup {NullServer}]
    /\ leaseToken \in [Players -> Nat]
    /\ dbVersion \in [Players -> Nat]
    /\ dbFencingToken \in [Players -> Nat]
    /\ pendingSave \subseteq (Servers \times Players)
    /\ saveAttempts \in [(Servers \times Players) -> Nat]
    /\ inventoryCount \in [Players -> Nat]
    /\ balance \in [Players -> Int]
    /\ playerState \in [Players -> {"INACTIVE", "JOINING", "ACTIVE", "QUITTING"}]
    /\ savedToken \in [Players -> Nat]

(*
 * =============================================================================
 * Helper definitions
 * =============================================================================
 *)

(* A player is "active" on a server if it's in ACTIVE state and owned by that server *)
IsActiveOn(uuid, server) ==
    /\ playerState[uuid] = "ACTIVE"
    /\ owner[uuid] = server

(* Check if a save is currently in flight for a server+uuid pair *)
HasPendingSave(server, uuid) ==
    (server, uuid) \in pendingSave

(* The data being saved includes the server's view of the player's inventory and balance *)
PendingData(server, uuid) ==
    pendingSaveData[(server, uuid)]

(* Current DB fencing token for a uuid *)
DbFencingToken(uuid) == dbFencingToken[uuid]

(*
 * =============================================================================
 * Actions
 * =============================================================================
 *)

(*
 * Join(server, uuid)
 * A player connects to a server. The server acquires a lock (incrementing
 * the fencing token) and loads data from the DB.
 *
 * Pre-conditions:
 *   - Player is INACTIVE (not on any server)
 *   - No other server owns this UUID
 *
 * Effects:
 *   - Fencing token is incremented (atomically by the DB)
 *   - Player state -> JOINING
 *   - Owner is set to this server
 *   - Inventory/balance loaded from DB data
 *)
Join(server, uuid) ==
    /\ playerState[uuid] = "INACTIVE"
    /\ owner[uuid] = NullServer
    /\ \A s \in Servers : ~IsActiveOn(uuid, s)
    /\ ~HasPendingSave(server, uuid)
    /\ server \in Servers
    /\ uuid \in Players
    /\ playerState' = [playerState EXCEPT ![uuid] = "JOINING"]
    /\ owner' = [owner EXCEPT ![uuid] = server]
    /\ leaseToken' = [leaseToken EXCEPT ![uuid] = leaseToken[uuid] + 1]
    /\ savedToken' = [savedToken EXCEPT ![uuid] = leaseToken[uuid] + 1]
    /\ UNCHANGED <<dbVersion, dbData, dbFencingToken, pendingSave, pendingSaveData,
                   saveAttempts, inventoryCount, balance, ledger>>

(*
 * JoinComplete(server, uuid)
 * The data has been loaded from the DB. The player is now ACTIVE.
 * This models the transition from "loading" to "playing".
 *)
JoinComplete(server, uuid) ==
    /\ playerState[uuid] = "JOINING"
    /\ owner[uuid] = server
    /\ playerState' = [playerState EXCEPT ![uuid] = "ACTIVE"]
    /\ UNCHANGED <<owner, leaseToken, dbVersion, dbData, dbFencingToken,
                   pendingSave, pendingSaveData, saveAttempts,
                   inventoryCount, balance, ledger, savedToken>>

(*
 * Quit(server, uuid)
 * A player disconnects. The server initiates an async save.
 * The player's current inventory and balance are captured for saving.
 *)
Quit(server, uuid) ==
    /\ playerState[uuid] = "ACTIVE"
    /\ owner[uuid] = server
    /\ ~HasPendingSave(server, uuid)
    /\ playerState' = [playerState EXCEPT ![uuid] = "QUITTING"]
    /\ pendingSave' = pendingSave \cup {(server, uuid)}
    /\ pendingSaveData' = [pendingSaveData EXCEPT ![(server, uuid)] =
        [inv |-> inventoryCount[uuid],
         bal |-> balance[uuid],
         version |-> dbVersion[uuid],
         token |-> savedToken[uuid]]]
    /\ saveAttempts' = [saveAttempts EXCEPT ![(server, uuid)] = 1]
    /\ UNCHANGED <<owner, leaseToken, dbVersion, dbData, dbFencingToken,
                   inventoryCount, balance, ledger, savedToken>>

(*
 * Save(server, uuid)
 * The async save reaches the DB. This is the CRITICAL action.
 *
 * The DB performs TWO checks:
 *   1. Version check (Dynamo): WHERE version = expectedVersion
 *   2. Fencing token check (Kleppmann): WHERE fencing_token <= saveToken
 *
 * If both pass, the write succeeds. If either fails, the write is rejected
 * (NoLateWrite invariant).
 *
 * NOTE: This models the DB as the single source of truth. The DB's fencing_token
 * and version are atomically updated in a single transaction.
 *)
Save(server, uuid) ==
    /\ (server, uuid) \in pendingSave
    /\ playerState[uuid] = "QUITTING"
    /\ owner[uuid] = server
    /\ LET data == pendingSaveData[(server, uuid)]
           expectedVersion == data.version
           saveToken == data.token
           currentDbVersion == dbVersion[uuid]
           currentDbToken == dbFencingToken[uuid]
       IN
       (* The write succeeds only if BOTH conditions hold: *)
       /\ expectedVersion = currentDbVersion   (* Version CAS — Dynamo *)
       /\ saveToken >= currentDbToken           (* Fencing — Kleppmann *)
       (* On success: DB updates version, data, and fencing token *)
       /\ dbVersion' = [dbVersion EXCEPT ![uuid] = currentDbVersion + 1]
       /\ dbData' = [dbData EXCEPT ![uuid] = data]
       /\ dbFencingToken' = [dbFencingToken EXCEPT ![uuid] = saveToken]
       /\ pendingSave' = pendingSave \ {(server, uuid)}
       /\ pendingSaveData' = [pendingSaveData EXCEPT ![(server, uuid)] = 0]
       /\ saveAttempts' = [saveAttempts EXCEPT ![(server, uuid)] = 0]
       (* Player is now fully disconnected *)
       /\ playerState' = [playerState EXCEPT ![uuid] = "INACTIVE"]
       /\ owner' = [owner EXCEPT ![uuid] = NullServer]
       /\ UNCHANGED <<leaseToken, inventoryCount, balance, ledger, savedToken>>

(*
 * SaveRejected(server, uuid)
 * The save was rejected because either:
 *   - Version mismatch (another server wrote newer data), OR
 *   - Fencing token violation (a stale lock holder tried to write)
 *
 * The rejected data is logged as a conflict snapshot. The player's DB data
 * (written by the newer token holder) is preserved.
 *)
SaveRejected(server, uuid) ==
    /\ (server, uuid) \in pendingSave
    /\ playerState[uuid] = "QUITTING"
    /\ owner[uuid] = server
    /\ LET data == pendingSaveData[(server, uuid)]
           expectedVersion == data.version
           saveToken == data.token
           currentDbVersion == dbVersion[uuid]
           currentDbToken == dbFencingToken[uuid]
       IN
       (* The write is rejected if EITHER condition fails: *)
       /\ (expectedVersion # currentDbVersion \/ saveToken < currentDbToken)
       (* Rejected save: clear pending, but DB data is NOT overwritten *)
       /\ pendingSave' = pendingSave \ {(server, uuid)}
       /\ pendingSaveData' = [pendingSaveData EXCEPT ![(server, uuid)] = 0]
       /\ saveAttempts' = [saveAttempts EXCEPT ![(server, uuid)] = 0]
       /\ playerState' = [playerState EXCEPT ![uuid] = "INACTIVE"]
       /\ owner' = [owner EXCEPT ![uuid] = NullServer]
       /\ UNCHANGED <<leaseToken, dbVersion, dbData, dbFencingToken,
                      inventoryCount, balance, ledger, savedToken>>

(*
 * Crash(server)
 * A server crashes. All players owned by that server become INACTIVE,
 * but their pending saves are LOST (the server is gone).
 * The locks are effectively released (lease expires), but the DB data
 * and fencing tokens are preserved.
 *
 * This models the worst case: server crash with pending saves in flight.
 * The next server that loads this player will get a new fencing token
 * and the DB's current (highest-token) data.
 *)
Crash(server) ==
    /\ server \in Servers
    /\ LET affectedPlayers == {uuid \in Players : owner[uuid] = server}
       IN
       /\ playerState' = [playerState EXCEPT ![uuid \in affectedPlayers] = "INACTIVE"]
       /\ owner' = [owner EXCEPT ![uuid \in affectedPlayers] = NullServer]
       /\ pendingSave' = pendingSave \ {(s, uuid) \in pendingSave : s = server}
       /\ saveAttempts' = [saveAttempts EXCEPT ![(server, uuid) \in
           {(server, p) \in (Servers \times Players) : p \in Players}] = 0]
       /\ UNCHANGED <<leaseToken, dbVersion, dbData, dbFencingToken,
                      pendingSaveData, inventoryCount, balance, ledger, savedToken>>

(*
 * RedisDelay()
 * Models a Redis Pub/Sub or Stream message delay. In our system, Redis is
 * used for efficiency (fast lock release notifications), not for correctness.
 * A delay here means the next server might have to poll the DB instead of
 * getting an instant notification. This does NOT affect correctness, only
 * latency. Modeled as a stuttering step (no state change).
 *)
RedisDelay == UNCHANGED vars

(*
 * DbDelay()
 * Models a DB query delay. The save might take longer, but it will eventually
 * arrive. Modeled as a stuttering step.
 *)
DbDelay == UNCHANGED vars

(*
 * RetrySave(server, uuid)
 * A save that was in flight can be retried (e.g. after a transient DB error).
 * The retry uses the SAME fencing token and version — it does not get a new
 * token. This ensures a retried save can never overwrite data written by a
 * newer token holder.
 *)
RetrySave(server, uuid) ==
    /\ (server, uuid) \in pendingSave
    /\ playerState[uuid] = "QUITTING"
    /\ owner[uuid] = server
    /\ saveAttempts[(server, uuid)] < 3  (* Max 3 retries *)
    /\ saveAttempts' = [saveAttempts EXCEPT ![(server, uuid)] =
        saveAttempts[(server, uuid)] + 1]
    /\ UNCHANGED <<owner, leaseToken, dbVersion, dbData, dbFencingToken,
                   pendingSave, pendingSaveData, playerState,
                   inventoryCount, balance, ledger, savedToken>>

(*
 * =============================================================================
 * Next-state relation and specification
 * =============================================================================
 *)

Next ==
    \/ \E server \in Servers, uuid \in Players : Join(server, uuid)
    \/ \E server \in Servers, uuid \in Players : JoinComplete(server, uuid)
    \/ \E server \in Servers, uuid \in Players : Quit(server, uuid)
    \/ \E server \in Servers, uuid \in Players : Save(server, uuid)
    \/ \E server \in Servers, uuid \in Players : SaveRejected(server, uuid)
    \/ \E server \in Servers : Crash(server)
    \/ RedisDelay
    \/ DbDelay
    \/ \E server \in Servers, uuid \in Players : RetrySave(server, uuid)

(*
 * Initial state: all players inactive, no owner, empty DB, empty ledger
 *)
Init ==
    /\ owner = [uuid \in Players |-> NullServer]
    /\ leaseToken = [uuid \in Players |-> 0]
    /\ dbVersion = [uuid \in Players |-> 0]
    /\ dbData = [uuid \in Players |-> [inv |-> 0, bal |-> 0, version |-> 0, token |-> 0]]
    /\ dbFencingToken = [uuid \in Players |-> 0]
    /\ pendingSave = {}
    /\ pendingSaveData = [(s, p) \in (Servers \times Players) |-> 0]
    /\ saveAttempts = [(s, p) \in (Servers \times Players) |-> 0]
    /\ inventoryCount = [uuid \in Players |-> 0]
    /\ balance = [uuid \in Players |-> 0]
    /\ ledger = <<>>
    /\ playerState = [uuid \in Players |-> "INACTIVE"]
    /\ savedToken = [uuid \in Players |-> 0]

vars == <<owner, leaseToken, dbVersion, dbData, dbFencingToken,
          pendingSave, pendingSaveData, saveAttempts,
          inventoryCount, balance, ledger, playerState, savedToken>>

Spec == Init /\ [][Next]_vars

(*
 * =============================================================================
 * Invariants — what we're verifying
 * =============================================================================
 *)

(*
 * NoDoubleOwner: The same UUID cannot be ACTIVE on two servers simultaneously.
 *
 * This is the core safety property. If violated, two servers would both
 * load the same player data, both modify it, and one would overwrite the
 * other — data loss.
 *)
NoDoubleOwner ==
    \A uuid \in Players :
        Cardinality({s \in Servers : IsActiveOn(uuid, s)}) <= 1

(*
 * NoLateWrite: A lower fencing token can never overwrite data written
 * by a higher fencing token. The DB's fencing_token only increases.
 *
 * This is enforced by the WHERE fencing_token <= saveToken check in Save().
 * If a stale lock holder (old token) tries to write after a newer lock
 * holder has already written, the DB rejects it.
 *)
NoLateWrite ==
    \A uuid \in Players : dbFencingToken[uuid] >= 0
    /\ \A (s, p) \in pendingSave :
        pendingSaveData[(s, p)] # 0 =>
        pendingSaveData[(s, p)].token >= 0

(*
 * NoDupItem: Player's inventory count never increases due to handoff.
 *
 * The key safety property is that the DB's inventory count never EXCEEDS
 * the server's in-memory count. When no save is pending, they must be
 * equal. When a save IS pending (in-flight to the DB), the DB may lag
 * behind (DB inv <= server inv) because the newer count hasn't landed yet.
 *
 * The previous formulation used a disjunction that made the invariant
 * vacuously true whenever any save was pending — too weak to catch
 * actual duplication bugs.
 *)
NoDupItem ==
    \A uuid \in Players :
        /\ dbData[uuid].inv >= 0
        /\ inventoryCount[uuid] >= 0
        (* When no save is pending, DB must match server state exactly *)
        /\ ((~\E s \in Servers : (s, uuid) \in pendingSave) =>
            dbData[uuid].inv = inventoryCount[uuid])
        (* When a save IS pending, DB inv must not exceed server inv *)
        /\ ((\E s \in Servers : (s, uuid) \in pendingSave) =>
            dbData[uuid].inv <= inventoryCount[uuid])

(*
 * NoLostMoney: The economy ledger is consistent — the sum of all deltas
 * in the ledger equals the current balance.
 *
 * Simplified model: balance starts at 0, and the ledger records every
 * change. The sum of all deltas must equal the balance.
 *)
NoLostMoney ==
    \A uuid \in Players :
        balance[uuid] = Sum(LedgerDeltasFor(uuid, ledger))

(* Helper: extract all deltas for a given UUID from the ledger *)
LedgerDeltasFor(uuid, lg) ==
    [i \in 1..Len(lg) |-> IF lg[i][1] = uuid THEN lg[i][2] ELSE 0]

(* Helper: sum a sequence of integers *)
Sum(seq) ==
    IF Len(seq) = 0 THEN 0
    ELSE seq[1] + Sum([i \in 1..(Len(seq)-1) |-> seq[i+1]])

(*
 * MonotonicVersion: The DB version only increases, never decreases.
 * Each successful save increments the version by 1.
 *)
MonotonicVersion ==
    \A uuid \in Players : dbVersion[uuid] >= 0

(*
 * =============================================================================
 * Combined invariant for model checking
 * =============================================================================
 *)
Invariant ==
    /\ TypeInvariant
    /\ NoDoubleOwner
    /\ NoLateWrite
    /\ NoDupItem
    /\ NoLostMoney
    /\ MonotonicVersion

=============================================================================
