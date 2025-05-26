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
        # Create gRPC channel with options for better debugging
        options = [
            ('grpc.keepalive_time_ms', 30000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_time_between_pings_ms', 10000),
            ('grpc.http2.min_ping_interval_without_data_ms', 300000)
        ]
        
        channel = grpc.aio.insecure_channel(entrypoint, options=options)
        
        # Create client stub
        client = shredstream_pb2_grpc.ShrederServiceStub(channel)
        
        print(f"Connecting to {entrypoint}...")
        
        # Test the connection first
        try:
            # Set a timeout for the connection test
            await asyncio.wait_for(channel.channel_ready(), timeout=10.0)
            print("✅ Channel is ready!")
        except asyncio.TimeoutError:
            print("❌ Connection timeout - server may be unreachable")
            return
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return
        
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
        
        print(f"📤 Sending subscription request with filter: {request.transactions}")
        
        # Create an async generator to send the request
        async def request_generator():
            yield request
        
        # Subscribe to transactions stream with timeout
        try:
            response_stream = client.SubscribeTransactions(request_generator())
            print("🔄 Waiting for messages...")
            
            message_count = 0
            
            # Add a timeout wrapper to detect if no messages are coming
            try:
                # Wait for the first message with a timeout
                first_message = await asyncio.wait_for(
                    response_stream.__anext__(), 
                    timeout=30.0
                )
                
                message_count += 1
                print(f"📨 Received first message #{message_count}")
                
                # Process the first message
                try:
                    if first_message.transaction and first_message.transaction.transaction:
                        transaction = first_message.transaction.transaction
                        
                        if transaction.signatures:
                            signature_bytes = transaction.signatures[0]
                            signature_b58 = base58.b58encode(signature_bytes).decode('utf-8')
                            print(f"Filters: {list(first_message.filters)}, Sig: {signature_b58}")
                        else:
                            print(f"Filters: {list(first_message.filters)}, Sig: No signatures")
                    else:
                        print(f"Filters: {list(first_message.filters)}, Sig: No transaction data")
                except Exception as e:
                    print(f"Error processing first message: {e}")
                
                # Continue with the rest of the stream
                print("🔄 Continuing to listen for more messages...")
                async for message in response_stream:
                    try:
                        message_count += 1
                        print(f"📨 Received message #{message_count}")
                        
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
                        
            except asyncio.TimeoutError:
                print("⏰ No messages received within 30 seconds")
                print("   This could mean:")
                print("   1. No transactions matching the filter are occurring")
                print("   2. The server is not sending data")
                print("   3. There's a network issue")
                print("   4. The filter criteria might be too restrictive")
                
                # Let's try to continue listening anyway
                print("🔄 Continuing to listen indefinitely...")
                async for message in response_stream:
                    try:
                        message_count += 1
                        print(f"📨 Received message #{message_count}")
                        
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
                        
            except StopAsyncIteration:
                print("🔚 Stream ended normally")
            except Exception as e:
                print(f"❌ Error in stream iteration: {e}")
                import traceback
                traceback.print_exc()
                
        except grpc.aio.AioRpcError as e:
            print(f"❌ gRPC stream error: {e}")
            print(f"   Status code: {e.code()}")
            print(f"   Details: {e.details()}")
        except Exception as e:
            print(f"❌ Unexpected stream error: {e}")
            import traceback
            traceback.print_exc()
                
    except grpc.aio.AioRpcError as e:
        print(f"❌ gRPC error: {e}")
        print(f"   Status code: {e.code()}")
        print(f"   Details: {e.details()}")
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'channel' in locals():
            print("🔌 Closing channel...")
            await channel.close()


def main():
    """Main function to run the transactions subscription."""
    try:
        asyncio.run(subscribe_transactions())
    except KeyboardInterrupt:
        print("\n👋 Exiting...")


if __name__ == "__main__":
    main() 