using System;
using System.Collections.Specialized;
using System.IO;
namespace ServiceBroker.Queues.Storage
{
	public static class SerializationExtensions
	{
		public static byte[] Serialize(this MessageEnvelope messageEnvelope)
		{
			using (var stream = new MemoryStream())
			using (var writer = new BinaryWriter(stream))
			{
				writer.Write(messageEnvelope.DeferProcessingUntilTime.HasValue);
				if(messageEnvelope.DeferProcessingUntilTime.HasValue)
					writer.Write(messageEnvelope.DeferProcessingUntilTime.Value.ToBinary());
				writer.Write(messageEnvelope.Data.Length);
				writer.Write(messageEnvelope.Data);
				writer.Write(messageEnvelope.Headers.Count);
				foreach (string key in messageEnvelope.Headers)
				{
					writer.Write(key);
					writer.Write(messageEnvelope.Headers[key]);
				}
				writer.Flush();
				return stream.ToArray();
			}
		}

		public static MessageEnvelope ToMessageEnvelope(this byte[] buffer)
		{
			using (var ms = new MemoryStream(buffer))
			using (var br = new BinaryReader(ms))
			{
				var messageEnvelope = new MessageEnvelope();
				if(br.ReadBoolean())
				{
					messageEnvelope.DeferProcessingUntilTime = DateTime.FromBinary(br.ReadInt64());
				}
				var length = br.ReadInt32();
				messageEnvelope.Data = br.ReadBytes(length);
				var headerCount = br.ReadInt32();
				messageEnvelope.Headers = new NameValueCollection(headerCount);
				for (var j = 0; j < headerCount; j++)
				{
					messageEnvelope.Headers.Add(br.ReadString(), br.ReadString());
				}
				return messageEnvelope;
			}
		}
	}
}