package org.apache.spark.network

import org.apache.spark.SparkConf
import java.nio.ByteBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.spark._
import org.apache.spark.util.{SystemClock, Utils}
import scala.io.Source
import java.util
import java.util.Random
import scala.collection.mutable.ArrayBuffer

/**
 * Created by brandon on 8/4/14.
 */
object Gups {
  def main(args:Array[String]) {
    if (args.length < 6) {
      println("usage: <myid> <hostsfile> <ppn> <numupdates> <Asize> <baseport>")
      System.exit(1)
    }

    val myid = args(0).toInt
    val hostsfile = args(1)
    val ppn = args(2).toInt
    val numUpdates = args(3).toLong
    val AGlobalSize = args(4).toLong
    // assign all different ports just to ensure same processes don't conflict
    val baseport = args(5).toInt


    val hosts = new util.ArrayList[ConnectionManagerId]
    var hostId = 0
    for (line <- Source.fromFile(hostsfile).getLines()) {
      for (pn <- 0 until ppn) {
        hosts.add(new ConnectionManagerId(line, baseport+hostId))
        hostId += 1
      }
    }

    // only way to let connection manager use the appropriate hostname
    org.apache.spark.util.Utils.setCustomHostname(hosts.get(myid).host)

    val nprocesses = hosts.size()
    val dist = new BlockDistribution(nprocesses, AGlobalSize);
    val Alocalsize = dist.getRangeForBlock(myid).size()
    if (Alocalsize > Integer.MAX_VALUE) throw new IllegalStateException("Alocalsize too large")
    val ALocal = Array.fill[Int](Alocalsize.toInt)(0)


    val conf = new SparkConf
    val manager = new ConnectionManager(hosts.get(myid).port, conf, new SecurityManager(conf))
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      val ind = msg.getChunkForReceiving(8).get.buffer.getLong();

      val localOffset = ind - dist.getRangeForBlock(myid).leftInclusive
      if (localOffset > Integer.MAX_VALUE) throw new IllegalStateException("got local offset too large")

      //println(myid + " Received [" + msg + "]("+ ind + ") to local offset " + localOffset + " from [" + id + "]")

      if (ind < dist.getRangeForBlock(myid).leftInclusive
        || ind >= dist.getRangeForBlock(myid).rightExclusive)
        throw new Exception("got " + localOffset + " from ["+ msg + "](" + ind + ") from [" + id + "]")

      ALocal(localOffset.toInt)+=1

      None
    })

    /////////////////////////////
    // Generate B
    val rand = new Random();
    System.out.print("Generating B[]...");
    val B = new ArrayBuffer[Long]()
    for (i <- 0L until numUpdates) {
      B += (rand.nextDouble()*AGlobalSize.toDouble).toLong
    }
    System.out.println("done");
    ////////////////////////////////


    /* testSequentialSending(manager) */
    /* System.gc() */

    // communication-free synchronization!!
    System.out.println("waiting")
    Thread.sleep(5000)
    System.out.println("starting")

    testParallelSending(manager, hosts, B.toArray, dist)
    println(ALocal.reduce(_ + _))
    /* System.gc() */

    /* testParallelDecreasingSending(manager) */
    /* System.gc() */

    //testContinuousSending(manager)
    System.gc()
  }

  def testParallelSending(manager: ConnectionManager,
                          hosts: util.ArrayList[ConnectionManagerId],
                          B: Array[Long],
                          dist: BlockDistribution) {
    println("--------------------------")
    println("Parallel Sending")
    println("--------------------------")
    val size = 8 //10 * 1024 * 1024

    val startTime = System.currentTimeMillis
    B.map(i => {
      val b = ByteBuffer.allocate(size).putLong(i)
      b.flip
      val targetid = dist.getBlockIdForIndex(i)
      if (targetid > Integer.MAX_VALUE) throw new IllegalStateException("targetid too large")
      val target = hosts.get(targetid.toInt)
      val bufferMessage = Message.createBufferMessage(b)
      manager.sendMessageReliably(target, bufferMessage)
    }).foreach(f => {
      val g = Await.result(f, 1 second)
      if (!g.isDefined) println("Failed")
    })
    val finishTime = System.currentTimeMillis

    val mb = size * B.length / 1024.0 / 1024.0
    val ms = finishTime - startTime
    val tput = mb * 1000.0 / ms
    println("--------------------------")
    println("Started at " + startTime + ", finished at " + finishTime)
    println("Sent " + B.length + " messages of size " + size + " in " + ms + " ms " +
      "(" + tput + " MB/s)")
    println("--------------------------")
    println()

    println("waiting to finish")
    Thread.sleep(5000)
    println("finish")
  }

}
