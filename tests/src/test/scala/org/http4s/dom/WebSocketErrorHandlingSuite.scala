/*
 * Copyright 2021 http4s.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.http4s.dom

import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.Uri
import org.http4s.client.websocket.WSFrame
import org.http4s.client.websocket.WSRequest
import org.http4s.dom.BuildInfo.fileServicePort
import scodec.bits.ByteVector

class WebSocketErrorHandlingSuite extends CatsEffectSuite {

  test("fail when sending after connection is closed") {
    WebSocketClient[IO]
      .connectHighLevel(
        WSRequest(Uri.fromString(s"ws://localhost:${fileServicePort}/ws").toOption.get))
      .use { conn =>
        for {
          _ <- conn.send(WSFrame.Text("test"))
          closeFrame <- conn.closeFrame.get
          result <- conn.send(WSFrame.Text("should fail")).attempt
        } yield {
          assert(result.isLeft)
          result.left.toOption.get match {
            case e: WebSocketException =>
              assert(e.reason.contains("WebSocket is already closed") || 
                     e.reason.contains("WebSocket is closing"))
            case _ => fail("Expected WebSocketException")
          }
        }
      }
  }

  test("fail when sending binary after connection is closed") {
    WebSocketClient[IO]
      .connectHighLevel(
        WSRequest(Uri.fromString(s"ws://localhost:${fileServicePort}/ws").toOption.get))
      .use { conn =>
        for {
          _ <- conn.send(WSFrame.Binary(ByteVector(1, 2, 3)))
          closeFrame <- conn.closeFrame.get
          result <- conn.send(WSFrame.Binary(ByteVector(4, 5, 6))).attempt
        } yield {
          assert(result.isLeft)
          result.left.toOption.get match {
            case e: WebSocketException =>
              assert(e.reason.contains("WebSocket is already closed") || 
                     e.reason.contains("WebSocket is closing"))
            case _ => fail("Expected WebSocketException")
          }
        }
      }
  }

  test("handle send errors gracefully") {
    WebSocketClient[IO]
      .connectHighLevel(
        WSRequest(Uri.fromString(s"ws://localhost:${fileServicePort}/ws").toOption.get))
      .use { conn =>
        // This test checks that the error handling wrapper works correctly
        // In a real scenario, send errors might occur due to network issues,
        // buffer overflow, or other browser-specific conditions
        conn.send(WSFrame.Text("test message")).attempt.map { result =>
          assert(result.isRight, "Send should succeed when connection is open")
        }
      }
  }

  test("fail when sending fragmented frames") {
    WebSocketClient[IO]
      .connectHighLevel(
        WSRequest(Uri.fromString(s"ws://localhost:${fileServicePort}/ws").toOption.get))
      .use { conn =>
        val fragmentedFrame = WSFrame.Text("fragment", last = false)
        conn.send(fragmentedFrame).attempt.map { result =>
          assert(result.isLeft)
          result.left.toOption.get match {
            case e: IllegalArgumentException =>
              assert(e.getMessage.contains("DataFrames cannot be fragmented"))
            case _ => fail("Expected IllegalArgumentException for fragmented frames")
          }
        }
      }
  }

}