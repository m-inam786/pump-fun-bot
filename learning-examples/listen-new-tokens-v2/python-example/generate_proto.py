#!/usr/bin/env python3
"""
Script to generate Python protobuf files from the shredstream.proto file.
Run this script to generate shredstream_pb2.py and shredstream_pb2_grpc.py
"""

import subprocess
import sys
import os
from pathlib import Path

def generate_proto_files():
    """Generate Python protobuf files from the proto definition."""
    
    # Get the current directory and proto file path
    current_dir = Path(__file__).parent
    proto_dir = current_dir.parent / "shreder-rust-example" / "proto"
    proto_file = proto_dir / "shredstream.proto"
    
    # Check if proto file exists
    if not proto_file.exists():
        print(f"Error: Proto file not found at {proto_file}")
        return False
    
    print(f"Generating Python protobuf files from {proto_file}")
    
    try:
        # Generate the Python protobuf files
        cmd = [
            sys.executable, "-m", "grpc_tools.protoc",
            f"--proto_path={proto_dir}",
            f"--python_out={current_dir}",
            f"--grpc_python_out={current_dir}",
            str(proto_file)
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        print("Successfully generated Python protobuf files:")
        print("- shredstream_pb2.py")
        print("- shredstream_pb2_grpc.py")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error generating protobuf files: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: grpc_tools not found. Please install it with:")
        print("pip install grpcio-tools")
        return False

def main():
    """Main function."""
    if generate_proto_files():
        print("\nProto files generated successfully!")
        print("You can now run the example scripts:")
        print("- python transactions_example.py")
        print("- python entries_example.py")
    else:
        print("\nFailed to generate proto files.")
        sys.exit(1)

if __name__ == "__main__":
    main() 