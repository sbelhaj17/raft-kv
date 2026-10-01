package raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static raftkv.raft.TestCluster.cmd;

import java.util.List;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;

import raftkv.raft.Message.AppendEntries;
import raftkv.raft.Message.AppendResponse;
import raftkv.raft.Message.RequestVote;
import raftkv.raft.Message.VoteResponse;

class RaftNodeTest {
    private static final RaftConfig CFG = RaftConfig.defaults();
    private static final int[] FIVE = {1, 2, 3, 4, 5};

    private static RaftNode node(int id, int[] members, RaftConfig cfg, HardState hs, Entry... entries) {
        return new RaftNode(id, members, cfg, new SplittableRandom(id), hs, Snapshot.EMPTY, List.of(entries));
    }

    private static Entry e(long index, long term) {
        return new Entry(term, index, cmd("x" + index + "@" + term));
    }

    private static <T extends Message> List<T> sent(Ready rd, Class<T> type) {
        return rd.messages().stream().filter(type::isInstance).map(type::cast).toList();
    }

    @Test
    void singleNodeElectsItselfAndCommitsAlone() {
        TestCluster c = new TestCluster(1, CFG, 1);
        RaftNode n = c.electLeader();
        n.propose(cmd("a"));
        c.deliverAll();
        List<Entry> applied = c.applied.get(1);
        assertEquals(2, applied.size(), "the no-op and the command");
        assertArrayEquals(cmd("a"), applied.get(1).data());
    }

    @Test
    void threeNodesAgreeOnOneLeader() {
        for (long seed = 0; seed < 20; seed++) {
            TestCluster c = new TestCluster(3, CFG, seed);
            RaftNode l = c.electLeader();
            c.tickAll(5);
            int leaders = 0;
            for (RaftNode n : c.nodes.values()) {
                if (n.isLeader()) leaders++;
                assertEquals(l.term(), n.term());
                assertEquals(l.id(), n.leader());
            }
            assertEquals(1, leaders);
        }
    }

    @Test
    void writesCommitOnEveryNode() {
        TestCluster c = new TestCluster(3, CFG, 7);
        RaftNode l = c.electLeader();
        for (int i = 0; i < 5; i++) l.propose(cmd("w" + i));
        c.deliverAll();
        c.tickAll(CFG.heartbeatTicks()); // followers learn the final commit index from a heartbeat
        for (RaftNode n : c.nodes.values()) {
            assertEquals(l.lastIndex(), n.commitIndex());
            List<Entry> applied = c.applied.get(n.id());
            assertArrayEquals(cmd("w4"), applied.get(applied.size() - 1).data());
        }
    }

    @Test
    void minorityCannotCommitButCatchesUpAfterHeal() {
        TestCluster c = new TestCluster(5, CFG, 3);
        RaftNode l = c.electLeader();
        long before = l.commitIndex();
        int[] others = c.nodes.keySet().stream().filter(id -> id != l.id()).mapToInt(Integer::intValue).toArray();
        c.isolated.add(others[0]);
        c.isolated.add(others[1]);
        c.isolated.add(others[2]);
        l.propose(cmd("lonely"));
        c.deliverAll();
        assertEquals(before, l.commitIndex(), "two of five is not a majority");

        c.isolated.clear();
        c.tickAll(CFG.heartbeatTicks());
        assertTrue(l.isLeader());
        assertEquals(l.lastIndex(), l.commitIndex());
    }

    @Test
    void votesOnlyForUpToDateCandidatesAndOncePerTerm() {
        RaftNode n = node(1, FIVE, CFG, new HardState(2, RaftNode.NONE, 0), e(1, 1), e(2, 2));

        n.step(new RequestVote(3, 2, 1, 5, 1));
        assertFalse(sent(n.ready(), VoteResponse.class).get(0).granted(), "longer log but older last term");

        n.step(new RequestVote(3, 3, 1, 1, 2));
        assertFalse(sent(n.ready(), VoteResponse.class).get(0).granted(), "same last term but shorter");

        n.step(new RequestVote(3, 4, 1, 2, 2));
        Ready rd = n.ready();
        assertTrue(sent(rd, VoteResponse.class).get(0).granted());
        assertEquals(new HardState(3, 4, 0), rd.hardState(), "the vote must be persisted before the reply goes out");

        n.step(new RequestVote(3, 5, 1, 9, 9));
        assertFalse(sent(n.ready(), VoteResponse.class).get(0).granted(), "already voted in term 3");
    }

    @Test
    void followerReplacesConflictingUncommittedTail() {
        RaftNode n = node(2, FIVE, CFG, new HardState(1, RaftNode.NONE, 1), e(1, 1), e(2, 1), e(3, 1));
        n.ready();
        Entry replacement = e(2, 2);
        n.step(new AppendEntries(2, 1, 2, 1, 1, List.of(replacement), 1, 0));
        Ready rd = n.ready();
        assertEquals(2, n.lastIndex());
        assertEquals(2, n.termAt(2));
        assertEquals(List.of(replacement), rd.entries(), "only the replaced entry needs writing");
        AppendResponse r = sent(rd, AppendResponse.class).get(0);
        assertTrue(r.success());
        assertEquals(2, r.index());
    }

    @Test
    void rejectionHintSkipsTheConflictingTerm() {
        RaftNode n = node(2, FIVE, CFG, new HardState(3, RaftNode.NONE, 1), e(1, 1), e(2, 3), e(3, 3), e(4, 3));
        n.ready();
        n.step(new AppendEntries(4, 1, 2, 4, 4, List.of(), 1, 0));
        AppendResponse r = sent(n.ready(), AppendResponse.class).get(0);
        assertFalse(r.success());
        assertEquals(2, r.hint(), "first index of term 3");

        n.step(new AppendEntries(4, 1, 2, 9, 4, List.of(), 1, 0));
        assertEquals(5, sent(n.ready(), AppendResponse.class).get(0).hint(), "just past a short log");
    }

    /**
     * Figure 8 of the Raft paper. The leader of term 4 holds an entry from term 2 at index 2 and
     * learns that a majority stores it. It must not commit it by counting, because a node holding
     * a term-3 entry at index 2 could still win an election and overwrite it.
     */
    @Test
    void leaderCountsReplicasOnlyForItsOwnTerm() {
        for (boolean unsafe : new boolean[] {false, true}) {
            RaftConfig cfg = CFG.withUnsafeCommitOldTerms(unsafe);
            RaftNode l = node(1, FIVE, cfg, new HardState(3, RaftNode.NONE, 1), e(1, 1), e(2, 2));
            while (!(l.role() == Role.CANDIDATE)) l.tick();
            assertEquals(4, l.term());
            l.step(new VoteResponse(4, 2, 1, true));
            l.step(new VoteResponse(4, 3, 1, true));
            assertTrue(l.isLeader());
            assertEquals(3, l.lastIndex(), "the no-op of term 4");
            l.ready();

            l.step(new AppendResponse(4, 2, 1, true, 2, 0, 0));
            l.step(new AppendResponse(4, 3, 1, true, 2, 0, 0));
            if (unsafe) {
                assertEquals(2, l.commitIndex(), "the deliberately broken rule commits by counting");
                continue;
            }
            assertEquals(1, l.commitIndex(), "index 2 is on three of five nodes but is from term 2");

            l.step(new AppendResponse(4, 2, 1, true, 3, 0, 0));
            l.step(new AppendResponse(4, 3, 1, true, 3, 0, 0));
            assertEquals(3, l.commitIndex(), "committing the term-4 entry commits index 2 with it");
        }
    }

    @Test
    void readIsConfirmedOnlyAfterAMajorityAnswers() {
        TestCluster c = new TestCluster(3, CFG, 11);
        RaftNode l = c.electLeader();
        c.deliverAll();
        long commit = l.commitIndex();

        int[] others = c.nodes.keySet().stream().filter(id -> id != l.id()).mapToInt(Integer::intValue).toArray();
        c.isolated.add(others[0]);
        c.isolated.add(others[1]);
        assertTrue(l.readIndex(42));
        c.tickAll(3 * CFG.heartbeatTicks());
        assertTrue(c.reads.get(l.id()).isEmpty(), "an isolated leader must not confirm reads");

        c.isolated.remove(others[0]);
        c.tickAll(CFG.heartbeatTicks());
        assertEquals(List.of(new ReadState(42, commit)), c.reads.get(l.id()));
    }

    @Test
    void newLeaderHoldsReadsUntilItCommitsInItsTerm() {
        TestCluster c = new TestCluster(3, CFG, 5);
        RaftNode l = null;
        // Tick one node at a time without delivering, until someone campaigns, then deliver only
        // the votes so the new leader's no-op has not been acknowledged yet.
        while (l == null) {
            for (RaftNode n : c.nodes.values()) {
                n.tick();
                if (n.role() == Role.CANDIDATE) {
                    for (Message m : n.ready().messages()) c.node(m.to()).step(m);
                    for (int id : c.nodes.keySet()) {
                        if (id == n.id()) continue;
                        for (Message m : c.node(id).ready().messages()) if (m.to() == n.id()) n.step(m);
                    }
                    l = n;
                    break;
                }
            }
        }
        assertTrue(l.isLeader());
        assertTrue(l.readIndex(1));
        Ready rd = l.ready();
        assertTrue(rd.reads().isEmpty());
        for (Message m : rd.messages()) c.node(m.to()).step(m);
        c.deliverAll();
        assertEquals(List.of(new ReadState(1, l.lastIndex())), c.reads.get(l.id()),
                "confirmed at the no-op's index, once it committed");
    }

    @Test
    void laggingFollowerIsSentASnapshot() {
        TestCluster c = new TestCluster(3, CFG, 9);
        RaftNode l = c.electLeader();
        int lagging = c.nodes.keySet().stream().filter(id -> id != l.id()).findFirst().orElseThrow();
        c.isolated.add(lagging);
        for (int i = 0; i < 10; i++) l.propose(cmd("v" + i));
        c.deliverAll();
        long snapIndex = l.commitIndex();
        Snapshot s = l.compact(snapIndex, cmd("state"));
        assertEquals(snapIndex, l.snapshotIndex());
        l.propose(cmd("after"));
        c.deliverAll();

        c.isolated.clear();
        c.tickAll(2 * CFG.heartbeatTicks());
        RaftNode f = c.node(lagging);
        assertEquals(List.of(s), c.snapshots.get(lagging));
        assertEquals(snapIndex, f.snapshotIndex());
        assertEquals(l.lastIndex(), f.lastIndex());
        assertEquals(l.commitIndex(), f.commitIndex());
        List<Entry> applied = c.applied.get(lagging);
        assertArrayEquals(cmd("after"), applied.get(applied.size() - 1).data());
        assertTrue(applied.stream().allMatch(x -> x.index() > snapIndex), "nothing below the snapshot is replayed");
    }

    @Test
    void restartRestoresTermVoteAndCommittedEntries() {
        HardState hs = new HardState(5, 3, 2);
        RaftNode n = node(1, FIVE, CFG, hs, e(1, 1), e(2, 4), e(3, 5));
        assertEquals(5, n.term());
        assertEquals(3, n.votedFor());
        Ready rd = n.ready();
        assertNull(rd.hardState(), "nothing changed, nothing to write");
        assertTrue(rd.entries().isEmpty());
        assertEquals(2, rd.committed().size(), "committed entries are replayed into the state machine");

        n.step(new RequestVote(5, 2, 1, 9, 9));
        assertFalse(sent(n.ready(), VoteResponse.class).get(0).granted(), "the vote for 3 survived the restart");
    }

    @Test
    void leaderStepsDownOnHigherTerm() {
        TestCluster c = new TestCluster(3, CFG, 2);
        RaftNode l = c.electLeader();
        int other = c.nodes.keySet().stream().filter(id -> id != l.id()).findFirst().orElseThrow();
        l.step(new AppendResponse(l.term() + 1, other, l.id(), false, 0, 0, 0));
        assertEquals(Role.FOLLOWER, l.role());
        assertFalse(l.propose(cmd("x")) > 0);
        assertFalse(l.readIndex(1));
    }

    @Test
    void staleRejectionDoesNotMoveNextBackwards() {
        TestCluster c = new TestCluster(3, CFG, 4);
        RaftNode l = c.electLeader();
        for (int i = 0; i < 3; i++) l.propose(cmd("x" + i));
        c.deliverAll();
        int f = c.nodes.keySet().stream().filter(id -> id != l.id()).findFirst().orElseThrow();
        // A rejection of prevIndex 1, sent long ago, arrives after the follower has matched everything.
        l.step(new AppendResponse(l.term(), f, l.id(), false, 1, 1, 0));
        Ready rd = l.ready();
        assertTrue(sent(rd, AppendEntries.class).isEmpty(), "nothing is resent for a stale rejection");
        assertNotNull(l);
    }
}
