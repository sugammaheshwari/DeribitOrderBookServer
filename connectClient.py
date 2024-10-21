import asyncio
import websockets
import json

async def subscribe_to_symbols(symbols):
    """Connect to the WebSocket server and subscribe to multiple symbols."""
    uri = "ws://localhost:5678"  # Adjust the URI if your server is running on a different address
    async with websockets.connect(uri) as websocket:
        # Subscribe to the specified symbols
        subscribe_message = json.dumps({"action": "subscribe", "symbols": symbols})
        await websocket.send(subscribe_message)
        print(f"Subscribed to {', '.join(symbols)}")

        try:
            # Listen for updates from the server
            while True:
                update = await websocket.recv()
                update_data = json.loads(update)
                print(f"Received update for {update_data['symbol']}: {update_data['order_book']}")
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed. Exiting...")

if __name__ == "__main__":
    # Define the symbols to subscribe to
    symbols_to_subscribe = ["BTC-PERPETUAL","ETH-25OCT24-2600-P","ETH-PERPETUAL","ETH_USDC-25OCT24-3600-C"]  # Change this list as needed

    asyncio.run(subscribe_to_symbols(symbols_to_subscribe))
