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

package io.openmessaging.storage.dledger.protocol;

import static io.openmessaging.storage.dledger.protocol.VoteResponse.RESULT.UNKNOWN;

public class VoteResponse extends RequestOrResponse {

    public RESULT voteResult = UNKNOWN;

    public VoteResponse() {

    }

    public VoteResponse(VoteRequest request) {
        copyBaseInfo(request);
    }

    public RESULT getVoteResult() {
        return voteResult;
    }

    public void setVoteResult(RESULT voteResult) {
        this.voteResult = voteResult;
    }

    public VoteResponse voteResult(RESULT voteResult) {
        this.voteResult = voteResult;
        return this;
    }

    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }

    /**
     * 所以要向获得赞成票，首先 term 需要在同一轮， 并且当前节点的ledgerTerm 和 ledgerEndIndex 不能小于远端节点
     * 同时广播的票据还需要尽快到达其它节点，不然节点可能已经投票了
     */
    public enum RESULT {
        UNKNOWN,
        ACCEPT, //赞成
        REJECT_UNKNOWN_LEADER,
        REJECT_UNEXPECTED_LEADER,
        REJECT_EXPIRED_VOTE_TERM, //拒绝票，原因是自己维护的投票轮次小于远端维护的投票轮次
        REJECT_ALREADY_VOTED, //拒绝票，投给了其它节点
        REJECT_ALREADY__HAS_LEADER, //拒绝票，原因是集群中存在leader 节点了
        REJECT_TERM_NOT_READY, //拒绝票，原因是对端的投票轮次小于自己的投票轮次，即对端还未准备好投票
        REJECT_TERM_SMALL_THAN_LEDGER, //拒绝票， 自己维护的term 小于远端维护的 ledgerEndTerm
        REJECT_EXPIRED_LEDGER_TERM,  //拒绝票，原因是自己维护的ledgerTerm小于对端维护的ledgerTerm
        REJECT_SMALL_LEDGER_END_INDEX; //决绝票 自己维护ledgerTerm 与 对端 ledgerTerm 相等，但是 ledgerEndIndex 小于对端维护的值
    }

    public enum ParseResult {
        WAIT_TO_REVOTE,
        REVOTE_IMMEDIATELY,
        PASSED,
        WAIT_TO_VOTE_NEXT;
    }
}
