/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * raft 选举的实现
 * 该类被实例化后 就会进入选举流程
 * 选举的流程主要和状态管理器 有关
 * 入口 startup()
 * 1.状态管理器初始化 那些东西 ?
 * 2.如何 发送投票请求和处理投票请求
 * 3.如何 心跳包的发送与处理
 *
 * 选举 状态变化 candidate --> 到达超时时间 --> 广播票据 --> 获取多数赞成票，成为leader
 * candidate 收到来自leader 心跳包  --> 变为follower （设置leaderId 为心跳包的id ）
 * follower 长时间没有收到心跳包 -->  变为candidate 重新进入选举
 *
 *
 * Ledger /ˈledʒə(r)/
 */
public class DLedgerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLedgerLeaderElector.class);

    //用来随机生成选举超时时间
    private Random random = new Random();
    private DLedgerConfig dLedgerConfig;
    private final MemberState memberState;
    //RPC服务，实现向集
    //群内的节点“发送心跳包、投票的RPC”
    private DLedgerRpcService dLedgerRpcService;

    //as a server handler
    //上次收到心跳包的时间戳
    private long lastLeaderHeartBeatTime = -1;
    //上次发送心跳包的时间戳
    private long lastSendHeartBeatTime = -1;
    //上次成功收到心跳包的时间戳 （主节点发送心跳包 成功的依据是 有半数节点回复成功 ）
    private long lastSuccHeartBeatTime = -1;
    //一个心跳包的周期，默认为2s。
    private int heartBeatTimeIntervalMs = 2000;
    /**
     * 允许最大的n个心跳周期内未收到心跳包，
     * 状态为Follower的节点只有超过maxHeartBeatLeak * heartBeatTimeIntervalMs的时间内未收到主节点的心跳包 (默认6s)，
     * 才会重新进入Candidate状态，进行下一轮选举
     */
    private int maxHeartBeatLeak = 3;
    //as a client
    //发送下一个心跳包的时间戳
    private long nextTimeToRequestVote = -1;
    //是否应该立即发起投票。 作用：从节点收到主节点心跳包，并且当前状态机轮次 大于主节点轮次
    //说明leader 的投票轮次 小于从节点轮次， 立即发起新投票请求
    private boolean needIncreaseTermImmediately = false;

    //最小的发送投票间隔时间，默认为300ms
    private int minVoteIntervalMs = 300;
    //最大的发送投票间隔时间，默认为1000ms
    private int maxVoteIntervalMs = 1000;

    //注册的节点状态处理器，通过addRoleChangeHandler方法添加  （扩展点，提供给外部扩展，例如节点由从节点变为主节点后 由通知）
    private List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();

    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
    //上一次投票的开销。
    private long lastVoteCost = 0L;
    //状态机管理器
    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);

    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;
        refreshIntervals(dLedgerConfig);
    }

    public void startup() {
        //是Leader选举内部维护的状态机，即维护节点状态在Follower、Candidate、Leader之间转换
        stateMaintainer.start();
        //依次启动注册的角色转换监听器，即内部状态机的状态发生变更后的事件监听器，是Leader选举的功能扩展点(事件通知具体做什么？)
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        //心跳间隔事件
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        //允许最大的n个心跳周期内未收到心跳包，
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        //最小的发送投票间隔时间
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        //最大的发送投票间隔时间
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {

        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        //请求投票轮次 小于 当前节点轮次 （则主节点发生过心跳超时 ）
        if (request.getTerm() < memberState.currTerm()) {
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.currTerm()) { //轮次相同
            if (request.getLeaderId().equals(memberState.getLeaderId())) {  //发送心跳包节点是当前节点主节点
                lastLeaderHeartBeatTime = System.currentTimeMillis(); //更新最后收到心跳包时间戳
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        // 异常情况 需要加锁确保线程安全
        synchronized (memberState) {
            if (request.getTerm() < memberState.currTerm()) { //发送心跳包节点轮次已经过期，需要重新进入选举
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.getLeaderId() == null) { //如果当前从节点维护主节点 id 为空，则使用主节点并返回成功
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {  //如果当前从节点维护主节点ID 与 发送心跳包节点id 不同，说明集群中存在另外一个Leader 节点（正常不应该发生）， 对端进入Candidate 状态
                    logger.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else { //如果发送心跳包节点投票轮次大于当前从节点投票轮次，则认为从节点还未准备好，进入candidate 状态，并立即发起一次投票
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
        }
    }

    //当修改role 为leader后，下一次会进入maintainAsLeader  方法
    public void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.currTerm() == term) {
                //设置role 和 leaderId为自己
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1; //因为是-1，会立即发送心跳包
                //角色状态转换事件
                handleRoleChange(term, MemberState.Role.LEADER);
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    /**
     * 方法复用 为自己投票 和 接受网络请求投票 都通过该方法处理
     * 逻辑：
     * 1.请求投票轮次 < 当前节点轮次， 投票直接为失败
     * 2.请求投票轮次 = 当前节点轮次， 判断是否投给请求节点 ，如果没有则告知原因
     * 3.请求投票轮次 > 当前节点轮次 告诉请求节点还未准备好
     *
     * @param request 定义一个投票请求对象
     * @param self
     * @return
     */
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) { //因为一个节点可能收到多个节点 投票请求
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm())
                        .voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }
            /**
             * step2： 判断发起节点，维护term 对投票进行 "仲裁"
             * 下面对发起节点 term 进行仲裁
             * 1. request.getTerm() < memberState.currTerm()
             * 发起投票节点term 小于 当前节点term， 直接拒绝
             * 2. request.getTerm() == memberState.currTerm()
             * 如果发起节点的term 等于 当前节点term，说明地位平等，查看该节点是否投过票
             * 2.1 如果未投票（currVoteFor == null） 或者已经投票给该节点，则继续后续逻辑(见step3)
             * 2.2 如果该节点已存在leader 节点，则拒绝并告知已存在leader 节点
             * 2.3 如果该节点还未有leader 节点，但已投给其它节点，拒绝请求节点，并告知已投票
             *
             * 3.如果发起投票节点term 大于当前节点term
             * 拒绝请求节点投票请求，并告知自身还未准备投票， 自身会使用请求节点投票轮次立即进入到candidate 状态
             *
             * step3：判断请求节点ledgerEndTerm 与 当前节点的ledgerEndTerm（这里主要是判断日志复制进度）
             * tips:只有当前节点未投过票 或者 已投给当前请求节点才进入
             */

            if (request.getTerm() < memberState.currTerm()) { //Raft 中，term 越大 越有话语权， 如果请求投票轮次 小于 当前节点投票轮次，则设置 REJECT_EXPIRED_VOTE_TERM
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            } else if (request.getTerm() == memberState.currTerm()) { //轮次相同，说明两个节点在同一轮投票。
                //memberState.currVoteFor() == null 未投票， 已投票给请求节点（memberState.currVoteFor().equals(request.getLeaderId())） 继续后续逻辑
                if (memberState.currVoteFor() == null) {
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //repeat just let it go
                } else {
                    //如果该节点已经存在leader 节点，则拒绝并告知已存在leader 节点
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY__HAS_LEADER));
                    } else { //该节点还未由leader ，但是currVoteFor 不为空，且不是请求的id， 标识投给其它节点了 （拒绝）。
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {  //大于 发起投票节点 轮次 大于 当前节点投票轮次 （request.getTerm() > memberState.currTerm() ）
                //拒绝发起投票节点 的请求，并告知还未准备好，然后使用发起投票节点的轮次进行candidate 状态
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }


            /**
             * step3 : 判断请求节点ledgerEndTerm 与 当前节点的ledgerEndTerm (判断日志复制进度)
             * 4.1如果请求节点的LedgerEndTerm 小于 当前节点的 LedgerEndTerm则拒绝，其原因是请求节点的复制进度比当前节点，这种清空不能成为主节点的
             * 4.2如果请求的LedgerEndTerm 相等，但是LedgerEndIndex 比当前节点小，则拒绝，原因同上一条
             * 4.3如果请求的term 小于 ledgerEndTerm 以同样的理由拒绝
             *
             */
            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEDGER_TERM));
            } else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }

            //发起节点的 LedgerEndTerm， term 大于等于 当前节点 LedgerEndTerm 并且请求节点复制日志进度 大于或者等于当前节点（LedgerEndIndex） 则投票给请求节点
            memberState.setCurrVoteFor(request.getLeaderId());
            //响应赞成票
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }

    private void sendHeartbeats(long term, String leaderId) throws Exception {
        final AtomicInteger allNum = new AtomicInteger(1); //集群内节点的个数
        final AtomicInteger succNum = new AtomicInteger(1); //收到成功响应的节点个数
        final AtomicInteger notReadyNum = new AtomicInteger(0); //收到对端没有准备好反馈的节点个数
        final AtomicLong maxTerm = new AtomicLong(-1); //当前集群中各个节点维护的最大的投票轮次
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false); //是否存在Leader节点不一致
        final CountDownLatch beatLatch = new CountDownLatch(1); //用于等待异步请求结果
        long startHeartbeatTimeMs = System.currentTimeMillis(); //本次心跳包开始发送的时间戳
        //循环集群中节点，发送心跳请求
        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {
                    //当收到一个节点响应 触发回调函数，统计响应结果
                    if (ex != null) {
                        throw ex;
                    }
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        case SUCCESS: //成功响应
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM: //节点的投票轮次小于从节点投票轮次（之前宕机了，然后重启？？）
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER: //从节点已经有了新的主节点
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY: //从节点未准备好
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }
                    //如果收到SUCCESS 节点超过集群节点半数，则唤醒主线程，进行后续流程
                    if (memberState.isQuorum(succNum.get())
                        || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("Parse heartbeat response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        beatLatch.countDown();
                    }
                }
            });
        }
        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        if (memberState.isQuorum(succNum.get())) {
            //更新发送心跳成功时间
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else { //心跳没有半数节点成功（可能出现问题了）
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) { //如果当前leader 收到SUCCESS 响应 + 未准备投票节点超过半数，则立即发送心跳包
                lastSendHeartBeatTime = -1;
            } else if (maxTerm.get() > term) { //如果从节点投票轮次比主节点还大，则使用从节点投票轮次
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) { //从节点已经有另外主节点 ，节点状态从leader 变为candidate
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                //有3个心跳周期都没有发送成功，leader 变为candidate
                changeRoleToCandidate(term);
            }
        }
    }

    /**
     * leader 节点需要做那些事情
     * 1. 需要按照固定频率发送心跳包
     *
     * @throws Exception
     */
    private void maintainAsLeader() throws Exception {
        //如果上一次发送心跳包 距离 现在过了 2s，则需要发送心跳包勒
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) { //step1: 如果当前时间与上一次发送心跳包 间隔 大于 一个心跳包周期，则进入发送心跳包逻辑
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) { //step2:
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();
                lastSendHeartBeatTime = System.currentTimeMillis(); //step3: 记录本次发送心跳包 时间戳
            }
            sendHeartbeats(term, leaderId); //step4: 向集群内"从节点" 发送心跳包
        }
    }

    /**
     * candidate 状态的节点收到leader 节点发送的心跳包后 会变为follower 状态
     * follower 如果超过3个心跳包周期没有收到心跳包 将变更为candidate （认为leader 可能失联了）
     * 上一次收到心跳包 距离 现在超过6s， 则会从follower 变为 candidate
     */
    private void maintainAsFollower() {
        //处于对性能的考虑 ，没有一上来就加锁，而是先判断 只有当上一次leader 心跳包 超过2个心跳包周期 才加锁 （同步锁定memberState）
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                if (memberState.isFollower() && (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    /**
     *
     * @param term 发起投票节点 维护 的选举轮次
     * @param ledgerEndTerm 发起投票节点维护最大投票轮次
     * @param ledgerEndIndex 发起投票节点维护最大日志条目索引
     * @return
     * @throws Exception
     */
    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long ledgerEndTerm,
        long ledgerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId()); //拉票，设置leaderId 为自己
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                //投自己一票
                voteResponse = handleVote(voteRequest, true);
            } else {
                //async 网络请求
                voteResponse = dLedgerRpcService.vote(voteRequest);
            }
            responses.add(voteResponse);

        }
        return responses;
    }

    /**
     * candidate 会调用该方法， 设置选举超时时间
     * 当前时间 + 最后投票花费时间（花费时间越小，则超时时间越短） + 最小投票间隔 300ms + （1000-300ms = 700ms内的随机值）
     * 也就是距离当前时间 1s 左右就可以请求选举，因为 lastVoteCost 比较少，可以先忽略
     * @return
     */
    private long getNextTimeToRequestVote() {
        //最小投票间隔之间随机值 ，raft 关键，每个节点投票超时时间引入了随机值
        return System.currentTimeMillis() + lastVoteCost + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    /**
     * 该方法会放入一个死循环中，除非被选为leader ，不然一致会进入该方法
     * 什么时候 candidate 变为 follower 勒？？
     *
     * 选举仲裁逻辑
     *
     * @throws Exception
     */
    private void maintainAsCandidate() throws Exception {
        //for candidate

        //如果当前事件 小于 下一次发起投票时间，并且步需要立即发起新一轮选举（心跳包会重置选举超时时间）
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        long term; //投票轮次
        long ledgerEndTerm;//Leader节点当前的投票轮次
        long ledgerEndIndex;//当前日志的最大序列，即下一条日志的开始index

        //这段代码 初始化term， ledgerEndTerm ledgerEndIndex
        synchronized (memberState) {
            if (!memberState.isCandidate()) {
                return;
            }
            //默认为等待下一轮投票 ， 如果上一次的投票结果为WAIT_TO_VOTE_NEXT（等待下一轮投票）或应该立即发起投票
            //如果是第一次投票，则term 为1
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                long prevTerm = memberState.currTerm();
                term = memberState.nextTerm(); //清空给voteFor其它节点投票,默认当前轮次+1
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {//如果上一次的投票结果不是WAIT_TO_VOTE_NEXT，则投票轮次依然为状态机内部维护好投票轮次（本质就是持久化文件中记录）
                term = memberState.currTerm();
            }
            ledgerEndIndex = memberState.getLedgerEndIndex();
            //Leader节点当前的投票轮次
            ledgerEndTerm = memberState.getLedgerEndTerm();
        }
        if (needIncreaseTermImmediately) {
            //需要立即下一轮投票，则重置立即投票标识， 并重新设置下一次投票时间（nextTimeToRequestVote）
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }

        long startVoteTimeMs = System.currentTimeMillis();
        //向多个节点发送投票请求（异步），然后返回了结果（并行请求 妙用）
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);
        final AtomicLong knownMaxTermInGroup = new AtomicLong(-1); //已知的最大投票轮次
        final AtomicInteger allNum = new AtomicInteger(0); //所有投票数
        final AtomicInteger validNum = new AtomicInteger(0); //有效投票数
        final AtomicInteger acceptedNum = new AtomicInteger(0); //赞成票数量
        final AtomicInteger notReadyTermNum = new AtomicInteger(0); //未准备投票的节点数量
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0); //发起投票节点的ledgerTerm小于 对端ledgerTerm 节点的个数， 例如发起投票节点LedgerEndTerm 为3，对端为4，表面都已经选出leader了
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false); //是否已存在 leader

        //下面异步写法，可以学习下，CompletableFuture + CountDownLatch 结合使用
        CountDownLatch voteLatch = new CountDownLatch(1);
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    //收到请求结果后 的具体处理流程 （※※※）
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        //收到响应则认为是有效的
                        validNum.incrementAndGet();
                    }
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED:
                                break;
                            case REJECT_ALREADY__HAS_LEADER: //已经存在leader了，无论判断其它投票结果，结束本轮投票
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                            case REJECT_EXPIRED_VOTE_TERM:
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEDGER_TERM:
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                biggerLedgerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;

                        }
                    }
                    if (alreadyHasLeader.get()
                        || memberState.isQuorum(acceptedNum.get())
                        || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("Get error when parsing vote response ", t);
                } finally {
                    allNum.incrementAndGet();
                    //收到所有节点的回复，唤醒同步等待线程
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            });

        }
        try {
            voteLatch.await(3000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }
        //记录请求投票花费时间
        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        //根据投票结果进行裁决，从而修改状态机 状态（例如获得多数投票，就更新解决为leader ）
        if (knownMaxTermInGroup.get() > term) { //自己的term 小于远端 维护的term，则更新当前term 为远端最大term
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote(); //重置超时时间
            changeRoleToCandidate(knownMaxTermInGroup.get());
        } else if (alreadyHasLeader.get()) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            //已经有leader，则更新下次选举时间为心跳 * 最大丢失心跳包 时间, 即默认6s 内没有收到一个心跳包， 则会重新发起投票选举
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs * maxHeartBeatLeak;
        } else if (!memberState.isQuorum(validNum.get())) { //有效票没有过半，新一轮投票
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (memberState.isQuorum(acceptedNum.get())) { //如果赞成过半，则状态为PASSED 后面要更新该节点为leader了
            parseResult = VoteResponse.ParseResult.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) { //如果赞成 + 还未准备好过半，则立即发起投票投票
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        } else if (memberState.isQuorum(acceptedNum.get() + biggerLedgerNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE; // WAIT_TO_REVOTE 该状态特征下次投票时不增加投票轮次
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else {
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        lastParseResult = parseResult; //保存最后投票结果
        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
            memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);

        if (parseResult == VoteResponse.ParseResult.PASSED) { //如果超过半数，则更新为leader
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            changeRoleToLeader(term);
        }

    }

    /**
     * The core method of maintainer.
     * Run the specified logic according to the current role:
     *  candidate => 候选者，该状态下的节点会发起投票，尝试选择自己为主节点，选举成功后，不会存在该状态下的节点。
     *  leader => 该状态下需要定时向从节点发送心跳包，用于传播数据、确保其领导地位
     *  follower => 该状态下会开启定时器，尝试进入Candidate状态，以便发起投票选举，一旦收到主节点的心跳包，则重置定时器
     *  状态机 不同的状态需要做的事情不同
     *
     *  初始状态为candidate
     * @throws Exception
     */
    private void maintainState() throws Exception {
        if (memberState.isLeader()) {
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            maintainAsFollower();
        } else {
            maintainAsCandidate();
        }
    }

    private void handleRoleChange(long term, MemberState.Role role) {
        //执行leader 变更事件
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }

    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    /**
     * 状态驱动器 maintainState
     * 继承至 ShutdownAbleThread，一个线程，调用该类start --> run --> doWork
     * run 方法是一个无线死循环，反复调用doWork
     */
    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override public void doWork() {
            try {
                //dLedgerConfig.isEnableLeaderElector()如果当前节点参与Leader选举 （默认为true ，即参与leader 选举），
                // 则调用maintainState()方法驱动状态机
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) {
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);
                    //maintainState 驱动状态机
                    DLedgerLeaderElector.this.maintainState();
                }
                //驱动一次休息10ms
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }

}
