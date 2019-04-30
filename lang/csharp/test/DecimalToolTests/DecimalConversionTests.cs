using System;
using Xunit;

using DecimalTools;


namespace DecimalToolTests
{
    /// <summary>
    /// Tests for decimal type conversion.
    ///
    /// The byte representations have been verified as consistent with the equivalent conversions in Java.
    /// </summary>
    public class DecimalConversionTests
    {
        // Test fixtures consistent with Java impl
        private string[] bufferValues =
        {
            "7048860F3AB2",
            "8FB779F0C54E",
            "FF000000000000000000000001",
            "00FFFFFFFFFFFFFFFFFFFFFFFF",
            "00"
        };

        private decimal[] decimalValues =
        {
            12345678910.1234m,
            -12345678910.1234m,
            Decimal.MinValue, // -79228162514264337593543950335
            Decimal.MaxValue, // 79228162514264337593543950335
            0m
        };

        private byte[] scales =
        {
            4,
            4,
            0,
            0,
            0
        };

        [Fact]
        public void ConvertFromBytesTest()
        {
            for (int i = 0; i < bufferValues.Length; i++)
            {       
                string hexData = bufferValues[i];
                decimal expectedValue = decimalValues[i]; 
            
                decimal outputValue = DecimalConversion.ConvertToLogicalValue(FromHexString(hexData), scales[i]);
                Assert.Equal(expectedValue, outputValue);
            }

        }

        
        [Fact]
        public void ConvertFromDecimalTest()
        {
            for (int i = 0; i < bufferValues.Length; i++)
            {
                decimal decimalValue = decimalValues[i]; 
                string expectedValue = bufferValues[i];
              
                byte[] outputValue = DecimalConversion.ConvertDecimalToBytes(decimalValue);
                Assert.Equal(expectedValue, BitConverter.ToString(outputValue).Replace("-", ""));
            }

        }

        [Fact]
        public void AssertMaxDecimalValueTest()
        {
            Assert.Equal("79228162514264337593543950335", Decimal.MaxValue.ToString());
        }

        [Fact]
        public void AssertMinDecimalValueTest()
        {
            Decimal d = 1.0m;
                       
            Assert.Equal("-79228162514264337593543950335", Decimal.MinValue.ToString());
        }
        
        
        public static byte[] FromHexString(string hexData)
        {
            if (hexData.Length % 2 != 0)
            {
                throw new ApplicationException("hexData must contain an even number of characters");
            }
            
            byte[] buffer = new byte[hexData.Length / 2];
            for (int i = 0; i < buffer.Length; i ++)
            {
                buffer[i] = Convert.ToByte(hexData.Substring(i * 2, 2), 16);
            }

            return buffer;
        }
   
    }
}