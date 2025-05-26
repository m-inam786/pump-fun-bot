#!/usr/bin/env python3
"""
Python equivalent of the Rust transactions example.
This script connects to the ShrederService and subscribes to transactions with filters.
"""

import asyncio
import grpc
import base58
import sys
from typing import Dict, List

# Import the generated protobuf files
try:
    import shredstream_pb2
    import shredstream_pb2_grpc
except ImportError:
    print("Error: Protobuf files not found. Please run 'python generate_proto.py' first.")
    sys.exit(1)


async def subscribe_transactions():
    """Subscribe to transactions stream with filters."""
    
    # Connect to the gRPC server
    entrypoint = "fra1.shreder.xyz:9991"
    
    try:
        # Create gRPC channel
        channel = grpc.aio.insecure_channel(entrypoint)
        
        # Create client stub
        client = shredstream_pb2_grpc.ShrederServiceStub(channel)
        
        print(f"Connecting to {entrypoint}...")
        print("Subscribing to transactions stream...")
        
        # Create the subscription request with filters
        # This is equivalent to the Rust hashmap with pumpfun filter
        filter_transactions = shredstream_pb2.SubscribeRequestFilterTransactions(
            account_exclude=[],
            account_include=[],
            account_required=["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"]
        )
        
        request = shredstream_pb2.SubscribeTransactionsRequest(
            transactions={"pumpfun": filter_transactions}
        )
        
        # Create an async generator to send the request
        async def request_generator():
            yield request
        
        # Subscribe to transactions stream
        response_stream = client.SubscribeTransactions(request_generator())
        
        async for message in response_stream:
            try:
                # Extract transaction data
                if message.transaction and message.transaction.transaction:
                    transaction = message.transaction.transaction
                    
                    # Get the first signature and encode it with base58
                    if transaction.signatures:
                        signature_bytes = transaction.signatures[0]
                        signature_b58 = base58.b58encode(signature_bytes).decode('utf-8')
                        
                        print(f"Filters: {list(message.filters)}, Sig: {signature_b58}")
                    else:
                        print(f"Filters: {list(message.filters)}, Sig: No signatures")
                else:
                    print(f"Filters: {list(message.filters)}, Sig: No transaction data")
                    
            except Exception as e:
                print(f"Error processing message: {e}")
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
    """Main function to run the transactions subscription."""
    try:
        asyncio.run(subscribe_transactions())
    except KeyboardInterrupt:
        print("\nExiting...")


if __name__ == "__main__":
    main() 