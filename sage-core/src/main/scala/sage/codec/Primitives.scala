package sage.codec

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}
import java.util.Arrays

import sage.Bytes
import sage.SageException.DecodeError

private[sage] object Primitives {

  private val Zero       = Bytes.utf8("0")
  private val One        = Bytes.utf8("1")
  private val MaxPreview = 64

  def encodeInt(value: Int): Bytes = encodeLong(value.toLong)

  def encodeLong(value: Long): Bytes =
    if (value == 0L) Zero
    else if (value == 1L) One
    else {
      val start  = if (value < 0) 1 else 0
      val digits = digitCount(value)
      val out    = new Array[Byte](start + digits)
      writeDigits(out, start, digits, value)
      if (start == 1) out(0) = '-'
      Bytes.wrap(IArray.unsafeFromArray(out))
    }

  // digits come from the negative magnitude, so Long.MinValue, which has no positive counterpart, is safe
  def digitCount(value: Long): Int = {
    val magnitude = if (value < 0) value else -value
    var digits    = 1
    var floor     = -10L
    while (digits < 19 && magnitude <= floor) {
      digits += 1
      floor *= 10
    }
    digits
  }

  def writeDigits(out: Array[Byte], start: Int, digits: Int, value: Long): Unit = {
    var magnitude = if (value < 0) value else -value
    var i         = start + digits - 1
    while (i >= start) {
      out(i) = ('0' - magnitude % 10).toByte
      magnitude /= 10
      i -= 1
    }
  }

  def encodeBoolean(value: Boolean): Bytes = if (value) One else Zero

  def decodeBoolean(bytes: Bytes): Either[DecodeError, Boolean] =
    if (bytes.sameBytes(One)) Right(true)
    else if (bytes.sameBytes(Zero)) Right(false)
    else Left(DecodeError("boolean (1 or 0)", preview(bytes)))

  def decodeUtf8(bytes: Bytes): Either[DecodeError, String] = {
    // Lenient decoding inserts U+FFFD for malformed input. If the decoded text does not contain U+FFFD, the bytes were valid and this method
    // can use the optimized String constructor. When U+FFFD is present, validate the bytes strictly.
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

  // ASCII digits with an optional '-', no '+', leading zeros or "-0", so distinct keys never decode to the same number
  def decodeLong(expected: String, min: Long, max: Long)(bytes: Bytes): Either[DecodeError, Long] = {
    val a        = bytes.unsafeArray
    val negative = a.length > 1 && a(0) == '-'
    var i        = if (negative) 1 else 0
    var ok       = i < a.length && (a(i) != '0' || a.length == 1)
    var acc      = 0L // accumulates the negated value so Long.MinValue fits
    while (ok && i < a.length) {
      val digit = a(i) - '0'
      ok = digit >= 0 && digit <= 9 && acc >= (Long.MinValue + digit) / 10
      if (ok) acc = acc * 10 - digit
      i += 1
    }
    val value    = if (negative) acc else -acc
    if (ok && (negative || acc != Long.MinValue) && value >= min && value <= max) Right(value)
    else Left(DecodeError(expected, preview(bytes)))
  }

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
