"use strict";

// μ-law decode/encode + (פשוט) upsample/downsample כדי להגיע ל-16k/24k
// MVP: מספיק כדי לשמוע קול. לא אופטימלי לאיכות.

function ulawByteToPcm16(sample) {
  sample = ~sample & 0xff;
  const sign = sample & 0x80;
  let exponent = (sample >> 4) & 0x07;
  let mantissa = sample & 0x0f;
  let pcm = ((mantissa << 3) + 0x84) << exponent;
  pcm -= 0x84;
  return sign ? -pcm : pcm;
}

function pcm16ToUlawByte(pcm) {
  const BIAS = 0x84;
  const CLIP = 32635;

  let sign = 0;
  if (pcm < 0) {
    sign = 0x80;
    pcm = -pcm;
  }
  if (pcm > CLIP) pcm = CLIP;
  pcm += BIAS;

  let exponent = 7;
  for (let expMask = 0x4000; (pcm & expMask) === 0 && exponent > 0; expMask >>= 1) {
    exponent--;
  }
  const mantissa = (pcm >> (exponent + 3)) & 0x0f;
  const ulaw = ~(sign | (exponent << 4) | mantissa);
  return ulaw & 0xff;
}

function b64ToBuf(b64) {
  return Buffer.from(b64, "base64");
}
function bufToB64(buf) {
  return Buffer.from(buf).toString("base64");
}

// ulaw8k -> pcm16k (simple 2x upsample by duplication)
function ulaw8kB64ToPcm16kB64(ulawB64) {
  const ulaw = b64ToBuf(ulawB64);
  const pcm16k = Buffer.alloc(ulaw.length * 2 * 2); // 2 bytes per sample, *2 for upsample
  let o = 0;
  for (let i = 0; i < ulaw.length; i++) {
    const s = ulawByteToPcm16(ulaw[i]); // ~14-bit-ish
    // write twice (upsample x2)
    pcm16k.writeInt16LE(s, o); o += 2;
    pcm16k.writeInt16LE(s, o); o += 2;
  }
  return bufToB64(pcm16k);
}

// pcm24k -> ulaw8k (downsample 3:1 + ulaw encode)
function pcm24kB64ToUlaw8kB64(pcmB64) {
  const pcm = b64ToBuf(pcmB64);
  const samples = pcm.length / 2;
  const outLen = Math.floor(samples / 3);
  const ulaw = Buffer.alloc(outLen);
  let oi = 0;
  for (let i = 0; i < samples; i += 3) {
    const s = pcm.readInt16LE(i * 2);
    ulaw[oi++] = pcm16ToUlawByte(s);
  }
  return bufToB64(ulaw);
}

module.exports = {
  ulaw8kB64ToPcm16kB64,
  pcm24kB64ToUlaw8kB64
};
