package sage.codec

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}
import java.util.Arrays

import sage.Bytes
import sage.SageException.DecodeError

private[codec] object Primitives {

  private val True       = Bytes.utf8("1")
  private val False      = Bytes.utf8("0")
  private val MaxPreview = 64

  def encodeNumber[A](value: A): Bytes = Bytes.utf8(value.toString)

  def encodeBoolean(value: Boolean): Bytes = if (value) True else False

  def decodeBoolean(bytes: Bytes): Either[DecodeError, Boolean] =
    if (bytes.sameBytes(True)) Right(true)
    else if (bytes.sameBytes(False)) Right(false)
    else Left(DecodeError("boolean (1 or 0)", preview(bytes)))

  def decodeUtf8(bytes: Bytes): Either[DecodeError, String] = {
    // lenient decoding always marks malformed input with U+FFFD, so its absence proves the bytes well-formed —
    // keeping the intrinsified String constructor on the hot path; only payloads containing U+FFFD re-validate strictly
    val text = bytes.asUtf8String
    if (text.indexOf('\uFFFD') < 0) Right(text) else strictDecodeUtf8(bytes)
  }

  private def strictDecodeUtf8(bytes: Bytes): Either[DecodeError, String] = {
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    // the decoder only reads, so wrapping the backing array is safe and avoids a copy
    try Right(decoder.decode(ByteBuffer.wrap(bytes.unsafeArray)).toString)
    catch { case _: CharacterCodingException => Left(DecodeError("UTF-8 string", preview(bytes))) }
  }

  def decodeNumber[A](expected: String, parse: String => Option[A])(bytes: Bytes): Either[DecodeError, A] =
    parse(bytes.asUtf8String).toRight(DecodeError(expected, preview(bytes)))

  // reject a leading '+' so "5"/"+5" don't decode to the same key
  def parseInt(text: String): Option[Int]   = if (text.startsWith("+")) None else text.toIntOption
  def parseLong(text: String): Option[Long] = if (text.startsWith("+")) None else text.toLongOption

  def preview(bytes: Bytes): String = {
    // MaxPreview code points never need more than 4 bytes each, so decoding a bounded window avoids
    // materializing a huge payload just to show its head
    val all    = bytes.unsafeArray
    val window = if (all.length <= MaxPreview * 4) all else Arrays.copyOfRange(all, 0, MaxPreview * 4)
    val text   = new String(window, StandardCharsets.UTF_8)
    val out    = new StringBuilder
    var i      = 0
    // appending whole code points and escapes keeps the cut from splitting a surrogate pair or an escape sequence
    while (i < text.length && out.length < MaxPreview) {
      val cp = text.codePointAt(i)
      if (cp == '\n') out.append("\\n")
      else if (cp == '\r') out.append("\\r")
      else if (cp == '\t') out.append("\\t")
      else if (Character.isISOControl(cp) || Character.getType(cp) == Character.FORMAT) out.append(f"\\u$cp%04x")
      else out.appendAll(Character.toChars(cp))
      i += Character.charCount(cp)
    }
    if (i < text.length || window.length < all.length) s"'$out…' (${bytes.length} bytes)" else s"'$out'"
  }
}
