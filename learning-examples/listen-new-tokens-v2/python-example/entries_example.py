#!/usr/bin/env python3
"""
Python equivalent of the Rust entries example.
This script connects to the ShrederService and subscribes to entries.
"""

import asyncio
import grpc
import pickle
import sys
from typing import List, Any

# Import the generated protobuf files
try:
    import shredstream_pb2
    import shredstream_pb2_grpc
except ImportError:
    print("Error: Protobuf files not found. Please run 'python generate_proto.py' first.")
    sys.exit(1)


class SolanaEntry:
    """Simple representation of a Solana entry for deserialization."""
    def __init__(self, data):
        self.transactions = data.get('transactions', [])


async def subscribe_entries():
    """Subscribe to entries stream and process them."""
    
    # Connect to the gRPC server
    entrypoint = "localhost:9991"
    
    try:
        # Create gRPC channel
        channel = grpc.aio.insecure_channel(entrypoint)
        
        # Create client stub
        client = shredstream_pb2_grpc.ShrederServiceStub(channel)
        
        # Create subscription request
        request = shredstream_pb2.SubscribeEntriesRequest()
        
        print(f"Connecting to {entrypoint}...")
        print("Subscribing to entries stream...")
        
        # Subscribe to entries stream
        async for slot_entry in client.SubscribeEntries(request):
            try:
                # Try to deserialize the entries data
                # Note: This is a simplified approach since we don't have the exact
                # Solana entry structure. In a real implementation, you'd need
                # to properly deserialize the Solana entry format.
                
                entries_data = slot_entry.entries
                
                # For demonstration, we'll assume the entries contain transaction data
                # In a real implementation, you'd need to properly parse the Solana entry format
                print(f"Slot: {slot_entry.slot}, Entries data size: {len(entries_data)} bytes")
                
                # If you have the exact Solana entry deserialization logic,
                # you would implement it here similar to the Rust version:
                # entries = deserialize_solana_entries(entries_data)
                # transaction_count = sum(len(entry.transactions) for entry in entries)
                # print(f"slot {slot_entry.slot}, entries: {len(entries)}, transactions: {transaction_count}")
                
            except Exception as e:
                print(f"Deserialization failed with error: {e}")
                continue
                
    except grpc.aio.AioRpcError as e:
        print(f"gRPC error: {e}")
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if 'channel' in locals():
            await channel.close()


def main():
    """Main function to run the entries subscription."""
    try:
        asyncio.run(subscribe_entries())
    except KeyboardInterrupt:
        print("\nExiting...")


if __name__ == "__main__":
    main() 