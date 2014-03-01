package eu.podval.datahackaton;

import com.google.bitcoin.core.*;
import com.google.bitcoin.params.MainNetParams;
import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.store.MemoryBlockStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.Future;

public class Analyzer {
    private static final Log logger = LogFactory.getLog(Analyzer.class);

    public static void main(String[] args) {
        final NetworkParameters params = MainNetParams.get();
        PeerGroup peerGroup = null;
        try {
            BlockStore blockStore = new MemoryBlockStore(params);
            BlockChain chain = new BlockChain(params, blockStore);
            peerGroup = new PeerGroup(params, chain);
            peerGroup.startAndWait();
            PeerAddress addr = new PeerAddress(InetAddress.getLocalHost(), params.getPort());
            peerGroup.addAddress(addr);
            peerGroup.waitForPeers(1).get();
            Peer peer = peerGroup.getConnectedPeers().get(0);

            int count = 0;
            BigInteger sum = BigInteger.valueOf(0);
            BigInteger nano = BigInteger.valueOf(100000000);

            Sha256Hash blockHashToGet = new Sha256Hash("000000000000034a7dedef4a161fa058a2d67a173a90155f3a2fe6fc132e0ebf");
            while (blockHashToGet != null) {
                Future<Block> future = peer.getBlock(blockHashToGet);
                Block block = future.get();

                for (Transaction t : block.getTransactions()) {
                    for(TransactionOutput o : t.getOutputs()) {
                        sum = sum.add(o.getValue());
//                        System.out.println("Value of a transaction is " + o.getValue());
                    }
                }

                // Get previous
                blockHashToGet = block.getPrevBlockHash();
                count++;

//                if(count % 1 == 0) {
                    System.out.println("Processing " + count + " transaction, total amount is " + sum.divide(nano) + " BTC.");
//                }
            }

                           /*
            Sha256Hash blockHash = new Sha256Hash("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f");
            Future<Block> future = peer.getBlock(blockHash);
            Block block = future.get();
            block.getPrevBlockHash()

            for (Transaction tx : block.getTransactions()) {
                System.out.print("TX ==> " + tx);
                for (TransactionInput i : tx.getInputs()) {
                    System.out.print("Input ==> " + i);
                    System.out.print("Value ==> " + i.);
                }
                for (TransactionOutput o : tx.getOutputs()) {
                    System.out.print("OutputValue ==> " + o.getValue());
                }
            }                */
        } catch (Exception e) {
            logger.error("Processing failed.", e);
        } finally {
            peerGroup.stop();
        }
    }
}
