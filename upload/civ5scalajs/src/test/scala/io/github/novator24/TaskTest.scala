package io.github.novator24

import org.scalatest.{FlatSpec, PrivateMethodTester}

// http://www.scalatest.org/user_guide/using_scalatest_with_sbt
// https://stackoverflow.com/a/24375762
class TaskTest extends FlatSpec with PrivateMethodTester {

  "Positive01" should "checkClients() correctly" in {
    val cls = new Task()
    val zero = Option[BigInt](0)
    val client = cls.Client("", zero, zero, zero, zero, zero)
    val stream = List(client).toStream
    val result = cls.checkClients(stream)
    assert(result.isEmpty)
  }

  "Positive02" should "checkOrders() correctly" in {
    val cls = new Task()
    val del = Option[Char]('d')
    val ten = Option[BigInt](10)
    val order = cls.Order("", del, "A", ten, ten)
    val stream = List(order).toStream
    val result = cls.checkOrders(stream)
    assert(result.isEmpty)
  }

  "Positive03" should "calcOrdersXOrders() correctly" in {
    val cls = new Task()
    val del = Option[Char]('d')
    val ten = Option[BigInt](10)
    val order = cls.Order("", del, "A", ten, ten)
    val stream = List(order).toStream
    val result = cls.calcOrdersXOrders(stream)
    assert(result.isEmpty)
  }

  "Positive04" should "calcOrdersXClients() correctly" in {
    val cls = new Task()
    val zero = Option[BigInt](0)
    val client = cls.Client("", zero, zero, zero, zero, zero)
    val stream1 = List(client, client.copy(name="Z")).toStream

    val buy = Option[Char]('b')
    val sell = Option[Char]('s')
    val ten = Option[BigInt](10)
    val order = cls.Order("", buy, "A", ten, ten)
    val stream2 = List(order, order.copy(letter = sell, clientName="Z")).toStream
    val queues = cls.calcOrdersXOrders(stream2)

    val result = cls.calcClientsXOrders(stream1, queues).toList
    assert(result.head.client.get.name === "")
    assert(result.head.client.get.total_usd.get === client.total_usd.get - ten.get * ten.get)
    assert(result.head.client.get.total_a_units.get === client.total_a_units.get + ten.get)
  }

  "Negative05" should "calcOrdersXClients() correctly" in {
    val cls = new Task()
    val zero = Option[BigInt](0)
    val client = cls.Client("", zero, zero, zero, zero, zero)
    val stream1 = List(client, client.copy(name="Z"), client.copy(name="Y")).toStream

    val buy = Option[Char]('b')
    val sell = Option[Char]('s')
    val ten = Option[BigInt](10)
    val order = cls.Order("", buy, "A", ten, ten)
    val stream2 = List(order.copy(letter = sell, clientName="Z"), order.copy(clientName="Y"), order).toStream
    val queues = cls.calcOrdersXOrders(stream2)

    val result = cls.calcClientsXOrders(stream1, queues).toList
    assert(result.head.client.get.name === "")
    assert(result.head.client.get.total_usd.get === client.total_usd.get)
    assert(result.head.client.get.total_a_units.get === client.total_a_units.get)
  }

}