package net.corda.node.services

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.TransactionType
import net.corda.core.contracts.UpgradedContract
import net.corda.core.contracts.requireThat
import net.corda.core.flows.*
import net.corda.core.identity.AnonymousPartyAndPath
import net.corda.core.identity.Party
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap

// TODO: We should have a whitelist of contracts we're willing to accept at all, and reject if the transaction
//       includes us in any outside that list. Potentially just if it includes any outside that list at all.
// TODO: Do we want to be able to reject specific transactions on more complex rules, for example reject incoming
//       cash without from unknown parties?
class NotifyTransactionHandler(val otherParty: Party) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        val request = receiveTransaction<BroadcastTransactionFlow.NotifyTxRequest>(otherParty).unwrap { it }
        serviceHub.recordTransactions(request.stx)
    }
}

class NotaryChangeHandler(otherSide: Party) : AbstractStateReplacementFlow.Acceptor<Party>(otherSide) {
    /**
     * Check the notary change proposal.
     *
     * For example, if the proposed new notary has the same behaviour (e.g. both are non-validating)
     * and is also in a geographically convenient location we can just automatically approve the change.
     * TODO: In more difficult cases this should call for human attention to manually verify and approve the proposal
     */
    override fun verifyProposal(proposal: AbstractStateReplacementFlow.Proposal<Party>): Unit {
        val state = proposal.stateRef
        val proposedTx = proposal.stx.tx

        if (proposedTx.type !is TransactionType.NotaryChange) {
            throw StateReplacementException("The proposed transaction is not a notary change transaction.")
        }

        val newNotary = proposal.modification
        val isNotary = serviceHub.networkMapCache.notaryNodes.any { it.notaryIdentity == newNotary }
        if (!isNotary) {
            throw StateReplacementException("The proposed node $newNotary does not run a Notary service")
        }
        if (state !in proposedTx.inputs) {
            throw StateReplacementException("The proposed state $state is not in the proposed transaction inputs")
        }

//            // An example requirement
//            val blacklist = listOf("Evil Notary")
//            checkProposal(newNotary.name !in blacklist) {
//                "The proposed new notary $newNotary is not trusted by the party"
//            }
    }
}

class ContractUpgradeHandler(otherSide: Party) : AbstractStateReplacementFlow.Acceptor<Class<out UpgradedContract<ContractState, *>>>(otherSide) {
    @Suspendable
    @Throws(StateReplacementException::class)
    override fun verifyProposal(proposal: AbstractStateReplacementFlow.Proposal<Class<out UpgradedContract<ContractState, *>>>) {
        // Retrieve signed transaction from our side, we will apply the upgrade logic to the transaction on our side, and
        // verify outputs matches the proposed upgrade.
        val stx = serviceHub.validatedTransactions.getTransaction(proposal.stateRef.txhash)
        requireNotNull(stx) { "We don't have a copy of the referenced state" }
        val oldStateAndRef = stx!!.tx.outRef<ContractState>(proposal.stateRef.index)
        val authorisedUpgrade = serviceHub.vaultService.getAuthorisedContractUpgrade(oldStateAndRef.ref) ?:
                throw IllegalStateException("Contract state upgrade is unauthorised. State hash : ${oldStateAndRef.ref}")
        val proposedTx = proposal.stx.tx
        val expectedTx = ContractUpgradeFlow.assembleBareTx(oldStateAndRef, proposal.modification).toWireTransaction()
        requireThat {
            "The instigator is one of the participants" using (otherSide in oldStateAndRef.state.data.participants)
            "The proposed upgrade ${proposal.modification.javaClass} is a trusted upgrade path" using (proposal.modification == authorisedUpgrade)
            "The proposed tx matches the expected tx for this upgrade" using (proposedTx == expectedTx)
        }
        ContractUpgradeFlow.verify(oldStateAndRef.state.data, expectedTx.outRef<ContractState>(0).state.data, expectedTx.commands.single())
    }
}

class TransactionKeyHandler(val otherSide: Party, val revocationEnabled: Boolean) : FlowLogic<Unit>() {
    constructor(otherSide: Party) : this(otherSide, false)

    companion object {
        object SENDING_KEY : ProgressTracker.Step("Sending key")
    }

    override val progressTracker: ProgressTracker = ProgressTracker(SENDING_KEY)

    @Suspendable
    override fun call(): Unit {
        val revocationEnabled = false
        progressTracker.currentStep = SENDING_KEY
        val legalIdentityAnonymous = serviceHub.keyManagementService.freshKeyAndCert(serviceHub.myInfo.legalIdentityAndCert, revocationEnabled)
        val otherSideAnonymous = sendAndReceive<AnonymousPartyAndPath>(otherSide, legalIdentityAnonymous).unwrap { TransactionKeyFlow.validateIdentity(otherSide, it) }
        // Validate then store their identity so that we can prove the key in the transaction is owned by the
        // counterparty.
        serviceHub.identityService.registerAnonymousIdentity(otherSideAnonymous, otherSide)
    }
}