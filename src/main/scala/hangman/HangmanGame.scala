// group 17
// 906984 Yerzhan Zhamashev
// 936323 Nikolai Semin

package hangman

import reactor.api.{EventHandler}
import reactor.Dispatcher
import hangman.util.TCPTextHandle
import hangman.util.AcceptHandle
import java.net.Socket

/** Represents the Hangman game.
  *
  * @param hiddenWord
  *   The word to be guessed in the game.
  * @param initialGuessCount
  *   The number of allowed incorrect guesses.
  * @param port
  *   Optional port number for the game server. If not specified, a default is
  *   used.
  */
class HangmanGame(
    val hiddenWord: String,
    val initialGuessCount: Int,
    port: Option[Int] = None
) {
  require(hiddenWord != null && hiddenWord.length > 0)
  require(initialGuessCount > 0)

  private var playerMessageHandlers = Set.empty[HangmanPlayerMessageHandler]
  var connectionHandler = new HangmanConnectionHandler(new AcceptHandle(port))
  val dispatcher = new Dispatcher(10)
  var gameState = new GameState(hiddenWord, initialGuessCount, Set.empty[Char])

  /** Starts the game by adding the server socket connection handler to the
    * dispatcher and start handling events. If the game is interrupted, it is
    * terminated. Otherwise, it is terminated when the game is over, i.e., there
    * are no more events to handle.
    */
  def start() {
    dispatcher.addHandler(connectionHandler)
    try {
      dispatcher.handleEvents()
    } catch {
      case e: InterruptedException => {
        terminate("Game interrupted")
      }
    }
  }

  /** Terminates the game and closes all connections and the server. Removes all
    * event handlers from the dispatcher. When handler is removed, the
    * dispatcher interrupts the associated worker thread.
    */
  def terminate(reason: String = ""): Unit = {
    if (reason != "") println(reason)
    println("Terminating game...")

    // Close all player connections and remove their handlers
    playerMessageHandlers.foreach(messageHandler => {
      messageHandler.getHandle.close()
      dispatcher.removeHandler(messageHandler)
    })

    // Close the server socket and remove the handler
    connectionHandler.getHandle.close()
    dispatcher.removeHandler(connectionHandler)

    println("Game terminated")
  }

  /** Adds a player to the game by adding their message handler to the
    * dispatcher.
    *
    * @param playerMessageHandler
    *   The message handler for the player.
    */
  private def addPlayer(
      playerMessageHandler: HangmanPlayerMessageHandler
  ): Unit = {
    println("Player connected")
    dispatcher.addHandler(playerMessageHandler)
    playerMessageHandlers = playerMessageHandlers + playerMessageHandler
  }

  /** Removes a player from the game by removing their message handler from the
    * dispatcher.
    *
    * @param playerMessageHandler
    *   The message handler for the player.
    */
  private def removePlayer(
      playerMessageHandler: HangmanPlayerMessageHandler
  ): Unit = {
    playerMessageHandler.getPlayerName match {
      case Some(name) => println(s"Player $name disconnected")
      case None       => println("Player disconnected before entering name")
    }
    playerMessageHandler.getHandle.close()
    dispatcher.removeHandler(playerMessageHandler)
    playerMessageHandlers = playerMessageHandlers - playerMessageHandler
  }

  /** Makes a guess in the game and sends the current game state to all players
    * who have identified themselves.
    *
    * @param guess
    *   The guess to be made.
    */
  private def makeGuess(guess: Char, playerName: String): Unit = {
    gameState = gameState.makeGuess(guess)
    playerMessageHandlers
      .filter(_.identifiedThemself)
      .foreach(messageHandler =>
        messageHandler.getHandle.write(
          s"$guess ${gameState.getMaskedWord} ${gameState.guessCount} $playerName"
        )
      )
    if (gameState.isGameOver) terminate("Game over")
  }

  /** Handles new connections for the Hangman game.
    *
    * @param handle
    *   The accept handle for the new connection.
    */
  final class HangmanConnectionHandler(val handle: AcceptHandle)
      extends EventHandler[Socket] {

    override def getHandle: AcceptHandle = handle

    /** Handles a new connection by creating a new player message handler.
      *
      * @param socket
      *   The socket for the new connection.
      */
    override def handleEvent(socket: Socket): Unit = socket match {
      case null => terminate("Server socket closed")
      case _ =>
        addPlayer(new HangmanPlayerMessageHandler(new TCPTextHandle(socket)))
    }
  }

  /** Handles messages from a player.
    *
    * @param handle
    *   The TCP text handle for the player.
    */
  final class HangmanPlayerMessageHandler(val handle: TCPTextHandle)
      extends EventHandler[String] {
    private var playerName = Option.empty[String]

    override def getHandle: TCPTextHandle = handle

    /** Handles a message from a player.
      *
      * @param message
      *   The message from the player.
      */
    override def handleEvent(message: String): Unit =
      // If the message is null, the player is disconnected and the socket is
      // closed
      message match {
        case null => removePlayer(this)
        case _    =>
          // If the player has not identified themself, consider the message to be
          // their name. Otherwise, consider the message to be a guess.
          this.playerName match {
            case None => this.handleNewPlayer(message)
            case Some(playerName) =>
              if (this.isMessageValid(message))
                makeGuess(message.charAt(0), playerName)
          }
      }

    /** Handles a new player by identifying them and sending the current game
      * state.
      *
      * @param message
      *   The name of the new player.
      */
    private def handleNewPlayer(message: String): Unit =
      if (this.isMessageValid(message)) {
        println("Player identified as " + message)
        this.getHandle.write(
          gameState.getMaskedWord + " " + gameState.guessCount
        )
        this.playerName = Option(message)
      }

    private def isMessageValid(message: String): Boolean =
      message != null && !message.isEmpty && message.forall(_.isLetter)

    def identifiedThemself: Boolean = this.playerName.isDefined

    def getPlayerName: Option[String] = this.playerName
  }
}

object HangmanGame {

  /** Starts the Hangman game.
    *
    * @param args
    *   The program arguments. The first argument is the word to be guessed, the
    *   second argument is the number of allowed incorrect guesses, and the
    *   third argument is the port number for the game server (optional).
    */
  def main(args: Array[String]): Unit = {
    val word: String = args(0) // first program argument
    val guessCount: Int = args(1).toInt // second program argument
    val port: Option[Int] = args.length match {
      case 3 => Option(args(2).toInt)
      case _ => None
    }

    val game: HangmanGame = new HangmanGame(word, guessCount, port)
    // start the game
    game.start()
  }

}
