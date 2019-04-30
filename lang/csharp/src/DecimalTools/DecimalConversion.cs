using System;
using System.Numerics;

namespace DecimalTools
{
    /// <summary>
    /// Tools to convert between .NET decimal types and BigEndian byte arrays according to the Avro logical type spec:
    ///
    ///     https://avro.apache.org/docs/1.8.2/spec.html#Decimal
    ///
    /// TODO:
    ///
    ///     -    Handle BigEndian architectures
    ///     -    Error handling and edge-case validation
    /// 
    /// </summary>
    public class DecimalConversion
    {
        public static byte[] ConvertDecimalToBytes(decimal logicalValue)
        {
            var val = (decimal)logicalValue;
            int[] valBits = decimal.GetBits(val);
            
            // We copy the decimal bytes into a little-endian byte[] buffer
            // This is padded with a 0 byte in the MSB, so that BigInteger interprets as an unsigned int
            byte[] buffer = new byte[16]; 
            
            if (BitConverter.IsLittleEndian)
            {
                Buffer.BlockCopy(valBits,0, buffer, 0, 12); 
            }
            else
            {
                throw new Exception();
            }
             
            // Buffer now contains a little-endian unsigned int
            BigInteger bigInt = new BigInteger(buffer);
            
            // If the decimal is negative, we need to invert the BigInteger
            if (logicalValue < 0)
            {
                bigInt = BigInteger.Negate(bigInt);                
            }

            // Now export as a BigEndian, signed int
            byte[] signedBytesBuffer = bigInt.ToByteArray();
            Array.Reverse(signedBytesBuffer);
            return signedBytesBuffer;
        }

        public static decimal ConvertToLogicalValue(byte[] unpaddedBuffer, byte scale)
        {
            // First get the unscaled value & sign
            Array.Reverse(unpaddedBuffer);
            BigInteger unscaledValue = new BigInteger(unpaddedBuffer);
            bool isNegative = unscaledValue < 0;

            // Use BigInteger to get the unsigned magnitude, then get the bytes representation:
            BigInteger absValue = BigInteger.Abs(unscaledValue);
            byte[] byte_values = absValue.ToByteArray();

            // We now copy the data into a fixed 12 byte buffer
            // TODO: should test for overflow here!
            byte[] padded_byte_values = new byte[12];
            Buffer.BlockCopy(byte_values, 0, padded_byte_values, 0, Math.Min(byte_values.Length,12));

            // Now we have the correct size buffer, we can extract the int parts required by the decimal constructor:
            int[] decimal_parts = new int[3];
            if (!BitConverter.IsLittleEndian)
            {
                // TODO: handle BigEndian arch
                throw new Exception();
            }
            else
            {
                decimal_parts[0] = BitConverter.ToInt32(padded_byte_values, 0);
                decimal_parts[1] = BitConverter.ToInt32(padded_byte_values, 4);
                decimal_parts[2] = BitConverter.ToInt32(padded_byte_values, 8);
            }

            return new decimal(decimal_parts[0], decimal_parts[1], decimal_parts[2],
                isNegative, scale);
        }

    }
}