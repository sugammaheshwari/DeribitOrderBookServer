import asyncio
import websockets
import json
import threading
from readWriteLock import ReadWriteLock
from collections import defaultdict

# Deribit Documentation : https://docs.deribit.com/next/?python#deribit-api-v2-1-1

# Data structures
order_books = defaultdict(dict)  # Store order book data per symbol
order_books_locks = defaultdict(ReadWriteLock)  # Create a read-write lock for each symbol's order book

subscribed_clients = defaultdict(set)  # Store which clients are subscribed to which symbols
subscribed_clients_lock = threading.Lock() # Lock for clientSocket symbol subscription list.

broadcast_queue = []  # Queue to hold order book broadcast updates
broadcast_queue_lock = threading.Lock()  # Lock for broadcast queue

# msg id global counter
msg_id = 0     # since only fetch_order_book is using this msg_id therefore no need to have a lock to access this variable.

# WebSocket authentication message for Deribit
auth_msg = {
    "jsonrpc": "2.0",
    "id": msg_id,
    "method": "public/auth",
    "params": {
        "grant_type": "client_credentials",
        "client_id": "9YgyTMFg",
        "client_secret": "9xYau6uFd8PWR6iHvQNCNM4mFWl2Pr84wu20lS6NePc"
    }
}

# Define the port for client connections
CLIENT_PORT = 5678

async def fetch_order_book():
    global msg_id
    async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
        # Authenticate
        msg_id = msg_id+1
        await websocket.send(json.dumps(auth_msg))

        response = await websocket.recv()
        response_data = json.loads(response)

        if 'result' in response_data and response_data['result']['access_token']:
            print("Authenticated with Deribit!")

            while True:
                # Get the list of subscribed symbols (keys of subscribed_clients)
                subscribed_symbols = list(subscribed_clients.keys())
                
                # For each symbol, subscribe and fetch order book
                for symbol in subscribed_symbols:

                    ob_msg = {
                        "jsonrpc" : "2.0",
                        "id" : msg_id,
                        "method" : "public/get_order_book",
                        "params" : {
                            "instrument_name" : symbol,
                            "depth" : 5
                        }
                    }

                    msg_id = msg_id+1
                    await websocket.send(json.dumps(ob_msg))

                    # Receive the update for the symbol
                    update = await websocket.recv()
                    update_data = json.loads(update)
                    print(f"Received update for {symbol}: {update_data}")
                    
                    # Acquire write lock for the symbol's order book
                    order_books_locks[symbol].acquire_write()
                    try:
                        # Update the order book for the symbol
                        order_books[symbol] = update_data['result']
                    finally:
                        order_books_locks[symbol].release_write()

                    # Queue the update for broadcasting
                    with broadcast_queue_lock:
                        broadcast_queue.append(symbol)
                    
                # Wait for 15 seconds before fetching the next batch of order books
                await asyncio.sleep(15)

async def handle_client(websocket, path):
    """Handle client connections and subscriptions."""
    print("Client connected.", websocket)

    async for message in websocket:
        data = json.loads(message)
        if 'action' in data:
            action = data['action']
            symbols = data.get('symbols', [])  # Get the list of symbols from the message

            if action == 'subscribe':
                with subscribed_clients_lock:  # Lock for accessing subscribed_clients
                    for symbol in symbols:
                        subscribed_clients[symbol].add(websocket)
                        print(f"Client subscribed to {symbol}")

            elif action == 'unsubscribe':
                with subscribed_clients_lock:  # Lock for accessing subscribed_clients
                    for symbol in symbols:
                        subscribed_clients[symbol].discard(websocket)
                        print(f"Client unsubscribed from {symbol}")


async def broadcast_order_book():
    """Broadcast order book updates to all subscribed clients."""
    print("starting orderbook broadcasting")
    while True:
        symbol = None
        with broadcast_queue_lock:
            if broadcast_queue:
                symbol = broadcast_queue.pop(0)  # Get the next symbol to broadcast

        if symbol is not None:
            # Acquire read lock to access the symbol's order book
            order_books_locks[symbol].acquire_read()
            data = {
                        "symbol": symbol,
                        "order_book": order_books[symbol]
                }
            order_books_locks[symbol].release_read()
        
            print("broadcasting data to clients for symbol:",symbol)
            if symbol in subscribed_clients:
                print()
                message = json.dumps(data)
                for client in subscribed_clients[symbol]:
                    try:
                        await client.send(message)
                    except Exception as e:
                        print(f"Error sending data to client: {e}")
        await asyncio.sleep(5)

def start_client_server():
    """Start the client WebSocket server."""
    loop = asyncio.new_event_loop()  # Create a new event loop
    asyncio.set_event_loop(loop)      # Set it as the current loop for this thread
    start_server = websockets.serve(handle_client, "localhost", CLIENT_PORT)
    
    try:
        loop.run_until_complete(start_server)  # Start the WebSocket server
        print(f"Client WebSocket server started on port {CLIENT_PORT}.")
        
        # Keep the server running
        loop.run_forever()
    finally:
        loop.close()  # Close the loop when done

def start_order_book_thread():
    """Start the thread that fetches order books from Deribit for all subscribed symbols."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(fetch_order_book())
    finally:
        loop.close()

def start_broadcast_thread():
    """Start the broadcasting thread for order book updates."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(broadcast_order_book())
    finally:
        loop.close()

def main():

    # Start the order book fetching thread
    order_book_thread = threading.Thread(target=start_order_book_thread, daemon=True)
    order_book_thread.start()

    # Start the broadcasting thread
    broadcast_thread = threading.Thread(target=start_broadcast_thread, daemon=True)
    broadcast_thread.start()

    # Start the client WebSocket server
    client_server_thread = threading.Thread(target=start_client_server, daemon=True)
    client_server_thread.start()

    # join threads before returning.
    order_book_thread.join()
    broadcast_thread.join()
    client_server_thread.join()

if __name__ == "__main__":
    main()