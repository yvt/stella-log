using System;

namespace Yavit.StellaDB.Ston
{
	static class DataTypes
	{
		public const byte Null = 0x00;
		public const byte Map = 0x01;
		public const byte List = 0x02;
		public const byte String8 = 0x03;
		public const byte String16 = 0x04;
		public const byte String24 = 0x05;
		public const byte String32 = 0x06;
		public const byte PositiveInteger8 = 0x0b;
		public const byte PositiveInteger16 = 0x0c;
		public const byte PositiveInteger24 = 0x0d;
		public const byte PositiveInteger32 = 0x0e;
		public const byte PositiveInteger48 = 0x0f;
		public const byte PositiveInteger64 = 0x10;
		public const byte NegativeInteger8 = 0x11;
		public const byte NegativeInteger16 = 0x12;
		public const byte NegativeInteger24 = 0x13;
		public const byte NegativeInteger32 = 0x14;
		public const byte NegativeInteger48 = 0x15;
		public const byte NegativeInteger64 = 0x16;
		public const byte True = 0x17;
		public const byte False = 0x18;
		public const byte Float = 0x19;
		public const byte Double = 0x1a;
		public const byte DateTime = 0x1b;
		public const byte ZeroInt = 0x1d;
		public const byte EmptyString = 0x1e;
		public const byte EOMLMarker = 0x1f; // end of list/map
		public const byte Char8 = 0x20;
		public const byte Char16 = 0x21;
		public const byte StringifiedSignedInteger8 = 0x22;
		public const byte StringifiedInteger8 = 0x23;
		public const byte StringifiedInteger16 = 0x24;
		public const byte StringifiedInteger32 = 0x25;
		public const byte StringifiedInteger64 = 0x26;
		public const byte IntegerBase = 0x80; // above this are 7-bit signed integers
	}
}

