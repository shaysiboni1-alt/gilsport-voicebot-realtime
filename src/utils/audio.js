"use strict";

// G.711 µ-law (PCMU) <-> PCM16LE helpers.
// Twilio Media Streams audio payload is typically 8kHz µ-law.
// Gemini Live expects 16kHz PCM16LE. We do a simple 2x upsample/downsample.

const BIAS = 0x84;
const CLIP = 32635;

function _ulawDecodeSample(uVal) {
  // uVal: 0..255
  uVal = (~uVal) & 0xff;
  const sign = uVal & 0x80;
  const exponent = (uVal >> 4) & 0x07;
  const mantissa = uVal & 0x0f;
  let sample = ((mantissa << 3) + BIAS) << exponent;
  sample -= BIAS;
  return sign ? -sample : sample;
}

function _ulawEncodeSample(pcm) {
  let sample = pcm;
  let sign = 0;
  if (sample < 0) {
    sign = 0x80;
    sample = -sample;
  }
  if (sample > CLIP) sample = CLIP;
  sample += BIAS;

  // Determine exponent.
  let exponent = 7;
  for (let exp = 0; exp < 8; exp++) {
    if (sample <= (0x1f << (exp + 3))) {
      exponent = exp;
      break;
    }
  }
  const mantissa = (sample >> (exponent + 3)) & 0x0f;
  const uVal = ~(sign | (exponent << 4) | mantissa) & 0xff;
  return uVal;
}

function _upsample2x(pcm8k) {
  // pcm8k: Int16Array
  const out = new Int16Array(pcm8k.length * 2);
  for (let i = 0; i < pcm8k.length; i++) {
    const s0 = pcm8k[i];
    const s1 = i + 1 < pcm8k.length ? pcm8k[i + 1] : s0;
    out[i * 2] = s0;
    // Linear interpolation midpoint
    out[i * 2 + 1] = ((s0 + s1) / 2) | 0;
  }
  return out;
}

function _downsample2x(pcm16k) {
  const outLen = Math.floor(pcm16k.length / 2);
  const out = new Int16Array(outLen);
  for (let i = 0; i < outLen; i++) out[i] = pcm16k[i * 2];
  return out;
}

function ulawToPcm16(ulawBuffer) {
  const u = Buffer.isBuffer(ulawBuffer) ? ulawBuffer : Buffer.from(ulawBuffer);
  const pcm8k = new Int16Array(u.length);
  for (let i = 0; i < u.length; i++) pcm8k[i] = _ulawDecodeSample(u[i]);
  const pcm16k = _upsample2x(pcm8k);
  return Buffer.from(pcm16k.buffer);
}

function pcm16ToUlaw(pcm16Buffer) {
  const b = Buffer.isBuffer(pcm16Buffer) ? pcm16Buffer : Buffer.from(pcm16Buffer);
  const pcm16k = new Int16Array(b.buffer, b.byteOffset, Math.floor(b.byteLength / 2));
  const pcm8k = _downsample2x(pcm16k);
  const out = Buffer.alloc(pcm8k.length);
  for (let i = 0; i < pcm8k.length; i++) out[i] = _ulawEncodeSample(pcm8k[i]);
  return out;
}

module.exports = {
  ulawToPcm16,
  pcm16ToUlaw,
};
