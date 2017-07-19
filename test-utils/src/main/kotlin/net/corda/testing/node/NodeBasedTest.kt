package net.corda.testing.node

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors.listeningDecorator
import net.corda.core.*
import net.corda.core.crypto.X509Utilities
import net.corda.core.crypto.appendToCommonName
import net.corda.core.crypto.commonName
import net.corda.core.internal.createDirectories
import net.corda.core.internal.div
import net.corda.core.node.services.ServiceInfo
import net.corda.core.node.services.ServiceType
import net.corda.core.utilities.WHITESPACE
import net.corda.node.internal.Node
import net.corda.node.serialization.NodeClock
import net.corda.node.services.config.ConfigHelper
import net.corda.node.services.config.FullNodeConfiguration
import net.corda.node.services.config.configOf
import net.corda.node.services.config.plus
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.transactions.RaftValidatingNotaryService
import net.corda.node.utilities.ServiceIdentityGenerator
import net.corda.nodeapi.User
import net.corda.nodeapi.config.parseAs
import net.corda.testing.DUMMY_MAP
import net.corda.testing.driver.addressMustNotBeBoundFuture
import net.corda.testing.getFreeLocalPorts
import org.apache.logging.log4j.Level
import org.bouncycastle.asn1.x500.X500Name
import org.junit.After
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.util.*
import java.util.concurrent.Executors
import kotlin.concurrent.thread

/**
 * Extend this class if you need to run nodes in a test. You could use the driver DSL but it's extremely slow for testing
 * purposes. Use the driver if you need to run the nodes in separate processes otherwise this class will suffice.
 */
// TODO Some of the logic here duplicates what's in the driver
abstract class NodeBasedTest {
    @Rule
    @JvmField
    val tempFolder = TemporaryFolder()

    private val nodes = ArrayList<Node>()
    private var _networkMapNode: Node? = null

    val networkMapNode: Node get() = _networkMapNode ?: startNetworkMapNode()

    init {
        System.setProperty("consoleLogLevel", Level.DEBUG.name().toLowerCase())
    }

    /**
     * Stops the network map node and all the nodes started by [startNode]. This is called automatically after each test
     * but can also be called manually within a test.
     */
    @After
    fun stopAllNodes() {
        val shutdownExecutor = listeningDecorator(Executors.newScheduledThreadPool(nodes.size))
        Futures.allAsList(nodes.map { shutdownExecutor.submit(it::stop) }).getOrThrow()
        // Wait until ports are released
        val portNotBoundChecks = nodes.flatMap {
            listOf(
                    it.configuration.p2pAddress.let { addressMustNotBeBoundFuture(shutdownExecutor, it) },
                    it.configuration.rpcAddress?.let { addressMustNotBeBoundFuture(shutdownExecutor, it) }
            )
        }.filterNotNull()
        nodes.clear()
        _networkMapNode = null
        Futures.allAsList(portNotBoundChecks).getOrThrow()
    }

    /**
     * You can use this method to start the network map node in a more customised manner. Otherwise it
     * will automatically be started with the default parameters.
     */
    fun startNetworkMapNode(legalName: X500Name = DUMMY_MAP.name,
                            platformVersion: Int = 1,
                            advertisedServices: Set<ServiceInfo> = emptySet(),
                            rpcUsers: List<User> = emptyList(),
                            configOverrides: Map<String, Any> = emptyMap()): Node {
        check(_networkMapNode == null || _networkMapNode!!.info.legalIdentity.name == legalName)
        return startNodeInternal(legalName, platformVersion, advertisedServices + ServiceInfo(NetworkMapService.type), rpcUsers, configOverrides).apply {
            _networkMapNode = this
        }
    }

    @JvmOverloads
    fun startNode(legalName: X500Name,
                  platformVersion: Int = 1,
                  advertisedServices: Set<ServiceInfo> = emptySet(),
                  rpcUsers: List<User> = emptyList(),
                  configOverrides: Map<String, Any> = emptyMap(),
                  noNetworkMap: Boolean = false,
                  waitForConnection: Boolean = true): ListenableFuture<Node> {
        val networkMapConf = if (!noNetworkMap) {
            mapOf(
                    "networkMapService" to mapOf(
                            "address" to networkMapNode.configuration.p2pAddress.toString(),
                            "legalName" to networkMapNode.info.legalIdentity.name.toString()
                    )
            )
        } else mapOf()
        val node = startNodeInternal(
                legalName,
                platformVersion,
                advertisedServices,
                rpcUsers,
                networkMapConf + configOverrides)
        return if (waitForConnection) node.nodeReadyFuture.map { node } else Futures.immediateFuture(node)
    }

    fun startNotaryCluster(notaryName: X500Name,
                           clusterSize: Int,
                           serviceType: ServiceType = RaftValidatingNotaryService.type): ListenableFuture<List<Node>> {
        ServiceIdentityGenerator.generateToDisk(
                (0 until clusterSize).map { baseDirectory(notaryName.appendToCommonName("-$it")) },
                serviceType.id,
                notaryName)

        val serviceInfo = ServiceInfo(serviceType, notaryName)
        val nodeAddresses = getFreeLocalPorts("localhost", clusterSize).map { it.toString() }

        val masterNodeFuture = startNode(
                X509Utilities.getX509Name("${notaryName.commonName}-0","London","demo@r3.com",null),
                advertisedServices = setOf(serviceInfo),
                configOverrides = mapOf("notaryNodeAddress" to nodeAddresses[0]))

        val remainingNodesFutures = (1 until clusterSize).map {
            startNode(
                    X509Utilities.getX509Name("${notaryName.commonName}-$it","London","demo@r3.com",null),
                    advertisedServices = setOf(serviceInfo),
                    configOverrides = mapOf(
                            "notaryNodeAddress" to nodeAddresses[it],
                            "notaryClusterAddresses" to listOf(nodeAddresses[0])))
        }

        return Futures.allAsList(remainingNodesFutures).flatMap { remainingNodes ->
            masterNodeFuture.map { masterNode -> listOf(masterNode) + remainingNodes }
        }
    }

    protected fun baseDirectory(legalName: X500Name) = tempFolder.root.toPath() / legalName.commonName.replace(WHITESPACE, "")

    private fun startNodeInternal(legalName: X500Name,
                                  platformVersion: Int,
                                  advertisedServices: Set<ServiceInfo>,
                                  rpcUsers: List<User>,
                                  configOverrides: Map<String, Any>): Node {
        val baseDirectory = baseDirectory(legalName).createDirectories()
        val localPort = getFreeLocalPorts("localhost", 2)
        val p2pAddress = configOverrides["p2pAddress"] ?: localPort[0].toString()
        val config = ConfigHelper.loadConfig(
                baseDirectory = baseDirectory,
                allowMissingConfig = true,
                configOverrides = configOf(
                        "myLegalName" to legalName.toString(),
                        "p2pAddress" to p2pAddress,
                        "rpcAddress" to localPort[1].toString(),
                        "extraAdvertisedServiceIds" to advertisedServices.map { it.toString() },
                        "rpcUsers" to rpcUsers.map { it.toMap() }
                ) + configOverrides
        )

        val parsedConfig = config.parseAs<FullNodeConfiguration>()
        val node = Node(parsedConfig, parsedConfig.calculateServices(), MOCK_VERSION_INFO.copy(platformVersion = platformVersion),
                if (parsedConfig.useTestClock) TestClock() else NodeClock())
        node.start()
        nodes += node
        thread(name = legalName.commonName) {
            node.run()
        }
        return node
    }
}
