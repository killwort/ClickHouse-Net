﻿using System;

namespace ClickHouse.Ado.Impl.Compress
{
    //This implementation is NOT COMPATIBLE with contemporary CityHash algo.
    //Shamelessly ripped and ported from clickhouse sources ./contrib/libcityhash/src/city.cc
    internal class ClickHouseCityHash
    {
        static ulong Fetch64(byte[] src, int off)
        {
            return BitConverter.ToUInt64(src, off);
        }

        static uint Fetch32(byte[] src, int off)
        {
            return BitConverter.ToUInt32(src, off);
        }

        // Some primes between 2^63 and 2^64 for various uses.
        const ulong k0 = 0xc3a5c85c97cb3127UL;
        const ulong k1 = 0xb492b66fbe98f273UL;
        const ulong k2 = 0x9ae16a3b2f90404fUL;
        const ulong k3 = 0xc949d7c7509e6557UL;

// Bitwise right rotate.  Normally this will compile to a single
// instruction, especially if the shift is a manifest constant.
        static ulong Rotate(ulong val, int shift)
        {
            // Avoid shifting by 64: doing so yields an undefined result.
            return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
        }

        // Equivalent to Rotate(), but requires the second arg to be non-zero.
        // On x86-64, and probably others, it's possible for this to compile
        // to a single instruction if both args are already in registers.
        static ulong RotateByAtLeast1(ulong val, int shift)
        {
            return (val >> shift) | (val << (64 - shift));
        }

        static ulong ShiftMix(ulong val)
        {
            return val ^ (val >> 47);
        }

        static ulong Hash128to64(UInt128 x)
        {
            // Murmur-inspired hashing.
            const ulong kMul = 0x9ddfea08eb382d69UL;
            ulong a = ((x.Low) ^ (x.High))*kMul;
            a ^= (a >> 47);
            ulong b = ((x.High) ^ a)*kMul;
            b ^= (b >> 47);
            b *= kMul;
            return b;
        }

        static ulong HashLen16(ulong u, ulong v)
        {
            return Hash128to64(new UInt128(u, v));
        }

        static ulong HashLen0to16(byte[] s, int len, int off)
        {
            if (len > 8)
            {
                ulong a = Fetch64(s, off);
                ulong b = Fetch64(s, off + len - 8);
                return HashLen16(a, RotateByAtLeast1(b + (ulong) len, len)) ^ b;
            }
            if (len >= 4)
            {
                ulong a = Fetch32(s, off);
                return HashLen16((ulong) len + (a << 3), Fetch32(s, off + len - 4));
            }
            if (len > 0)
            {
                byte a = s[off];
                byte b = s[off + (len >> 1)];
                byte c = s[off + len - 1];
                uint y = (uint) (a) + ((uint) (b) << 8);
                uint z = (uint) (len + ((uint) (c) << 2));
                return ShiftMix(y*k2 ^ z*k3)*k2;
            }
            return k2;
        }

        // This probably works well for 16-byte strings as well, but it may be overkill
        // in that case.
        static ulong HashLen17to32(byte[] s, int len)
        {
            ulong a = Fetch64(s, 0)*k1;
            ulong b = Fetch64(s, 8);
            ulong c = Fetch64(s, +len - 8)*k2;
            ulong d = Fetch64(s, +len - 16)*k0;
            return HashLen16(Rotate(a - b, 43) + Rotate(c, 30) + d, a + Rotate(b ^ k3, 20) - c + (ulong) len);
        }

        // Return a 16-byte hash for 48 bytes.  Quick and dirty.
        // Callers do best to use "random-looking" values for a and b.
        static UInt128 WeakHashLen32WithSeeds(
            ulong w, ulong x, ulong y, ulong z, ulong a, ulong b)
        {
            a += w;
            b = Rotate(b + a + z, 21);
            ulong c = a;
            a += x;
            a += y;
            b += Rotate(a, 44);
            return new UInt128(a + z, b + c);
        }

        // Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
        static UInt128 WeakHashLen32WithSeeds(
            byte[] s, ulong a, ulong b, int off)
        {
            return WeakHashLen32WithSeeds(Fetch64(s, off),
                Fetch64(s, off + 8),
                Fetch64(s, off + 16),
                Fetch64(s, off + 24),
                a,
                b);
        }

        // Return an 8-byte hash for 33 to 64 bytes.
        static ulong HashLen33to64(byte[] s, int len)
        {
            ulong z = Fetch64(s, +24);
            ulong a = Fetch64(s, 0) + ((ulong) len + Fetch64(s, +len - 16))*k0;
            ulong b = Rotate(a + z, 52);
            ulong c = Rotate(a, 37);
            a += Fetch64(s, +8);
            c += Rotate(a, 7);
            a += Fetch64(s, +16);
            ulong vf = a + z;
            ulong vs = b + Rotate(a, 31) + c;
            a = Fetch64(s, +16) + Fetch64(s, +len - 32);
            z = Fetch64(s, +len - 8);
            b = Rotate(a + z, 52);
            c = Rotate(a, 37);
            a += Fetch64(s, +len - 24);
            c += Rotate(a, 7);
            a += Fetch64(s, +len - 16);
            ulong wf = a + z;
            ulong ws = b + Rotate(a, 31) + c;
            ulong r = ShiftMix((vf + ws)*k2 + (wf + vs)*k0);
            return ShiftMix(r*k0 + vs)*k2;
        }

        ulong CityHash64(byte[] s, int len)
        {
            if (len <= 32)
            {
                if (len <= 16)
                {
                    return HashLen0to16(s, len, 0);
                }
                else
                {
                    return HashLen17to32(s, len);
                }
            }
            else if (len <= 64)
            {
                return HashLen33to64(s, len);
            }

            // For strings over 64 bytes we hash the end first, and then as we
            // loop we keep 56 bytes of state: v, w, x, y, and z.
            ulong x = Fetch64(s, 0);
            ulong y = Fetch64(s, +len - 16) ^ k1;
            ulong z = Fetch64(s, +len - 56) ^ k0;
            UInt128 v = WeakHashLen32WithSeeds(s, (ulong) len, y, +len - 64);
            UInt128 w = WeakHashLen32WithSeeds(s, (ulong) len*k1, k0, +len - 32);
            z += ShiftMix(v.High)*k1;
            x = Rotate(z + x, 39)*k1;
            y = Rotate(y, 33)*k1;

            // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
            len = (len - 1) & ~(int) (63);
            ulong swp;
            int off = 0;
            do
            {
                x = Rotate(x + y + v.Low + Fetch64(s, off + 16), 37)*k1;
                y = Rotate(y + v.High + Fetch64(s, off + 48), 42)*k1;
                x ^= w.High;
                y ^= v.Low;
                z = Rotate(z ^ w.Low, 33);
                v = WeakHashLen32WithSeeds(s, v.High*k1, x + w.Low, off);
                w = WeakHashLen32WithSeeds(s, z + w.High, y, off + 32);
                swp = z;
                z = x;
                x = swp;
                off += 64;
                len -= 64;
            } while (len != 0);
            return HashLen16(HashLen16(v.Low, w.Low) + ShiftMix(y)*k1 + z,
                HashLen16(v.High, w.High) + x);
        }

        ulong CityHash64WithSeed(byte[] s, int len, ulong seed)
        {
            return CityHash64WithSeeds(s, len, k2, seed);
        }

        ulong CityHash64WithSeeds(byte[] s, int len,
            ulong seed0, ulong seed1)
        {
            return HashLen16(CityHash64(s, len) - seed0, seed1);
        }

        // A subroutine for CityHash128().  Returns a decent 128-bit hash for strings
        // of any length representable in sint.  Based on City and Murmur.
        static UInt128 CityMurmur(byte[] s, int len, UInt128 seed, int off)
        {
            ulong a = seed.Low;
            ulong b = seed.High;
            ulong c = 0;
            ulong d = 0;
            int l = len - 16;
            if (l <= 0)
            {
                // len <= 16
                a = ShiftMix(a*k1)*k1;
                c = b*k1 + HashLen0to16(s, len, off);
                d = ShiftMix(a + (len >= 8 ? Fetch64(s, off) : c));
            }
            else
            {
                // len > 16
                c = HashLen16(Fetch64(s, off + len - 8) + k1, a);
                d = HashLen16(b + (ulong) len, c + Fetch64(s, off + len - 16));
                a += d;
                do
                {
                    a ^= ShiftMix(Fetch64(s, off)*k1)*k1;
                    a *= k1;
                    b ^= a;
                    c ^= ShiftMix(Fetch64(s, off + 8)*k1)*k1;
                    c *= k1;
                    d ^= c;
                    off += 16;
                    l -= 16;
                } while (l > 0);
            }
            a = HashLen16(a, c);
            b = HashLen16(d, b);
            return new UInt128(a ^ b, HashLen16(b, a));
        }

        static UInt128 CityHash128WithSeed(byte[] s, int len, UInt128 seed, int off)
        {
            if (len < 128)
            {
                return CityMurmur(s, len, seed, off);
            }

            // We expect len >= 128 to be the common case.  Keep 56 bytes of state:
            // v, w, x, y, and z.
            UInt128 v = new UInt128(), w = new UInt128();
            ulong x = seed.Low;
            ulong y = seed.High;
            ulong z = (ulong) len*k1;
            v.Low = Rotate(y ^ k1, 49)*k1 + Fetch64(s, off);
            v.High = Rotate(v.Low, 42)*k1 + Fetch64(s, off + 8);
            w.Low = Rotate(y + z, 35)*k1 + x;
            w.High = Rotate(x + Fetch64(s, off + 88), 53)*k1;

            // This is the same inner loop as CityHash64(), manually unrolled.
            ulong swp;
            do
            {
                x = Rotate(x + y + v.Low + Fetch64(s, off + 16), 37)*k1;
                y = Rotate(y + v.High + Fetch64(s, off + 48), 42)*k1;
                x ^= w.High;
                y ^= v.Low;
                z = Rotate(z ^ w.Low, 33);
                v = WeakHashLen32WithSeeds(s, v.High*k1, x + w.Low, off);
                w = WeakHashLen32WithSeeds(s, z + w.High, y, off + 32);
                swp = z;
                z = x;
                x = swp;
                off += 64;
                x = Rotate(x + y + v.Low + Fetch64(s, off + 16), 37)*k1;
                y = Rotate(y + v.High + Fetch64(s, off + 48), 42)*k1;
                x ^= w.High;
                y ^= v.Low;
                z = Rotate(z ^ w.Low, 33);
                v = WeakHashLen32WithSeeds(s, v.High*k1, x + w.Low, off);
                w = WeakHashLen32WithSeeds(s, z + w.High, y, off + 32);
                swp = z;
                z = x;
                x = swp;
                off += 64;
                len -= 128;
            } while ((len >= 128));
            y += Rotate(w.Low, 37)*k0 + z;
            x += Rotate(v.Low + z, 49)*k0;
            // If 0 < len < 128, hash up to 4 chunks of 32 bytes each from the end of s.
            for (int tail_done = 0; tail_done < len;)
            {
                tail_done += 32;
                y = Rotate(y - x, 42)*k0 + v.High;
                w.Low += Fetch64(s, off + len - tail_done + 16);
                x = Rotate(x, 49)*k0 + w.Low;
                w.Low += v.Low;
                v = WeakHashLen32WithSeeds(s, v.Low, v.High, off + len - tail_done);
            }
            // At this point our 48 bytes of state should contain more than
            // enough information for a strong 128-bit hash.  We use two
            // different 48-byte-to-8-byte hashes to get a 16-byte final result.
            x = HashLen16(x, v.Low);
            y = HashLen16(y, w.Low);
            return new UInt128(HashLen16(x + v.High, w.High) + y,
                HashLen16(x + w.High, y + v.High));
        }

        public static UInt128 CityHash128(byte[] val)
        {
            return CityHash128(val, val.Length);
        }

        static UInt128 CityHash128(byte[] value, int len)
        {
            if (value.Length >= 16)
                return CityHash128WithSeed(value, len - 16, new UInt128(Fetch64(value, 0) ^ k3, Fetch64(value, 8)), 16);
            if (value.Length >= 8)
            {
                var seed = new UInt128(Fetch64(value, 0) ^ ((ulong) value.Length*k0), Fetch64(value, value.Length - 8) ^ k1);
                return CityHash128WithSeed(value, 0, seed, 8);
            }
            return CityHash128WithSeed(value, len, new UInt128(k0, k1), 0);

        }
    }
}