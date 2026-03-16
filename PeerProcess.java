import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.ByteBuffer;
import java.time.Clock;

public class PeerProcess {

    static final String HEADER = "P2PFILESHARINGPROJ";

    static Map<Integer, int[]> neighborBitfields = Collections.synchronizedMap(new HashMap<>());

    static Set<Integer> interestedPeers = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> peersChokingMe = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> peersIAmChoking = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> peersWithFullFile = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> preferredNeighbors = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> requestedPieces = Collections.synchronizedSet(new HashSet<>());

    static Map<Integer, ConnectionHandler> connectionsByPeerID = Collections.synchronizedMap(new HashMap<>());

    static Integer optimisticUnchockedPeer;

    static int[] myBitfield;

    static Logger currentPeerLogger;

    static class Configure {
        int numPreferredNeighbors;
        int unchokingInterval;
        int optimisticUnchokingInterval;
        String fileName;
        int fileSize;
        int pieceSize;

        Configure(int n, int ui, int oui, String name, int fs, int ps) {
            this.numPreferredNeighbors = n;
            this.unchokingInterval = ui;
            this.optimisticUnchokingInterval = oui;
            this.fileName = name;
            this.fileSize = fs;
            this.pieceSize = ps;
        }
    }

    static class Message {
        int length;
        byte type;
        byte[] messagePayload;

        Message(int length, byte type, byte[] messagePayload) {
            this.length = length;
            this.type = type;
            this.messagePayload = messagePayload;
        }
    }

    static class Logger {
        int peerId;
        Clock loggerClock;
        String logFile;

        Logger(int peerId) {
            this.peerId = peerId;
            this.loggerClock = Clock.systemUTC();
            this.logFile = "./log_peer_" + this.peerId + ".log";
        }

        void logOutboundTCPConnection(int neighborID) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId + " makes a connection to Peer "
                        + neighborID + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to write log." + e.getMessage());
            }
        }

        void logInboundTCPConnection(int neighborID) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId + " is connected from Peer "
                        + neighborID + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to write log." + e.getMessage());
            }
        }

        void logInterestedPeer(int neighborID) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId
                        + " received the 'interested' message from " + neighborID + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }

        void logUninterestedPeer(int neighborID) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId
                        + " received the 'not interested' message from " + neighborID + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }

        void logChokedByPeer(int neighborID) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId
                        + " is choked by " + neighborID + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }

        void logUnchokedByPeer(int neighborID) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId
                        + " is unchoked by " + neighborID + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }
    }

    static class ConnectionHandler implements Runnable {
        int peerID;
        Socket socket;
        DataInputStream in;
        DataOutputStream out;

        ConnectionHandler(int peerID, Socket socket, DataInputStream in, DataOutputStream out) throws IOException {
            this.peerID = peerID;
            this.socket = socket;
            this.in = in;
            this.out = out;
        }

        void sendMessage(int messageType, byte[] messagePayload) throws IOException {
            int messageLength = 1 + ((messagePayload == null) ? 0 : messagePayload.length);

            ByteBuffer buffer = ByteBuffer.allocate(4 + messageLength);
            buffer.putInt(messageLength);
            buffer.put((byte) messageType);

            if (messagePayload != null) {
                buffer.put(messagePayload);
            }

            this.out.write(buffer.array());
            this.out.flush();
        }

        void sendBitfield() throws IOException {
            sendMessage(5, bitfieldToPayload(myBitfield));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Message msg = readMessage(in);
                    processMessage(msg, this.peerID);
                }
            } catch (IOException e) {
                System.out.println("[INFO] Connection close with peer #" + peerID);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Stores peer ID and port for each connected peer
    // -------------------------------------------------------------------------
    static class PeerInfo {
        int id;
        String hostname;
        int port;
        int file;

        PeerInfo(int id) {
            this.id = id;
            this.port = -1;
            this.hostname = "";
            this.file = -1;
        };

        @Override
        public String toString() {
            return "Peer[id=" + id + ", port=" + port + ", hostname=" + hostname + ", hasFile=" + file + "]";
        }
    }

    // Active peers: peerID -> PeerInfo
    static Map<Integer, PeerInfo> earlierPeers = new HashMap<>();
    static Map<Integer, PeerInfo> allPeers = new HashMap<>();

    // -------------------------------------------------------------------------
    // Handshake: send 18 header + 10 zeros + 4 peerID = 32 bytes
    // We add our port so the remote side can save it
    // -------------------------------------------------------------------------

    static void sendHandshake(DataOutputStream out, int myID) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.put(HEADER.getBytes()); // 18 bytes
        buffer.put(new byte[10]); // 10 zero bytes
        buffer.putInt(myID); // 4 bytes — our peerID

        out.write(buffer.array());
        out.flush();
    }

    static int readHandshake(DataInputStream in) throws IOException {
        byte[] received = new byte[32];

        in.readFully(received);

        String theirHeader = new String(received, 0, 18, "UTF-8");

        if (!HEADER.equals(theirHeader)) {
            throw new IOException("Bad handshake header: " + theirHeader);
        }

        return ByteBuffer.wrap(received, 28, 4).getInt();
    }

    static Message readMessage(DataInputStream in) throws IOException {
        // This reads 4 bytes so it works for getting the length.
        int length = in.readInt();

        if (length < 1) {
            throw new IOException("[ERROR] Invalid message. Message length: " + length);
        }

        byte messageType = in.readByte();

        int payloadLength = length - 1;
        byte[] payload = new byte[payloadLength];

        if (payloadLength > 0) {
            in.readFully(payload);
        }

        return new Message(length, messageType, payload);
    }

    static void handleIncomingConnection(Socket socket, int myPeerID, Logger logger, Map<Integer, PeerInfo> allPeers) {
        try {
            BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
            DataInputStream in = new DataInputStream(bis);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            int theirID = readHandshake(in);

            if (allPeers.get(theirID) == null) {
                System.out.println("[ERROR] Invalid peer is attempting to connect with ID: " + theirID);
                try {
                    socket.close();
                } catch (Exception e) {
                    System.out.println("[ERROR] Couldn't close connection: " + e.getMessage());
                }

                return;
            }

            logger.logInboundTCPConnection(theirID);

            if (connectionsByPeerID.containsKey(theirID)) {
                System.out.println("[WARN] There is a duplicate connection being made with peer #" + theirID);
                socket.close();
                return;
            }

            sendHandshake(out, myPeerID);

            ConnectionHandler handler = new ConnectionHandler(theirID, socket, in, out);

            connectionsByPeerID.put(theirID, handler);

            handler.sendBitfield();

            new Thread(handler).start();
        } catch (EOFException e) {
            System.out.println("[INFO] Incoming connection close by other peer.");
        } catch (IOException e) {
            System.out.println("[ERROR] Connection failed.\n" + e.getMessage());
            try {
                socket.close();
            } catch (IOException ignore) {
            }
        }
    }

    // TODO: Finish the implementation for processing ccinoming messages.
    // TODO: Add the additional logging (beyond new TCP connections) for the
    // incoming messages and other events.

    static void processMessage(Message msg, int peerID) {
        switch (msg.type) {
            case 0:
                peersChokingMe.add(peerID);
                currentPeerLogger.logChokedByPeer(peerID);
                break;
            case 1:
                peersChokingMe.remove(peerID);
                currentPeerLogger.logUnchokedByPeer(peerID);
                break;
            case 2:
                interestedPeers.add(peerID);
                currentPeerLogger.logInterestedPeer(peerID);
                break;
            case 3:
                interestedPeers.remove(peerID);
                currentPeerLogger.logUninterestedPeer(peerID);
                break;
            case 4:
                break;
            case 5:
                int[] receivedBitfield = payloadToBitfield(msg.messagePayload);
                neighborBitfields.put(peerID, receivedBitfield);

                try {
                    if (isInterested(receivedBitfield)) {
                        connectionsByPeerID.get(peerID).sendMessage(2, null);
                    } else {
                        connectionsByPeerID.get(peerID).sendMessage(3, null);
                    }
                } catch (Exception e) {
                    System.out.println("[ERROR] " + e.getMessage());
                }

                break;
            case 6:
                break;
            case 7:
                break;
            default:
                throw new Error("[ERROR] Received improper message: " + msg.toString());
        }
    }

    static byte[] bitfieldToPayload(int[] bitfield) {
        byte[] payload = new byte[bitfield.length];

        for (int i = 0; i < bitfield.length; i++) {
            payload[i] = (byte) bitfield[i];
        }

        return payload;
    }

    static int[] payloadToBitfield(byte[] payload) {
        int[] bitfield = new int[payload.length];

        for (int i = 0; i < payload.length; i++) {
            bitfield[i] = (int) payload[i];
        }

        return bitfield;
    }

    static boolean isInterested(int[] otherBitfield) throws Exception {
        if (otherBitfield.length != myBitfield.length) {
            throw new Exception("[ERROR] Two peers have different bitfield sizes.");
        }

        for (int i = 0; i < myBitfield.length; i++) {
            if (myBitfield[i] == 0 && otherBitfield[i] == 1) {
                return true;
            }
        }

        return false;
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        // TODO: We need to track:
        // - download rates for each peer in current interval,
        if (args.length != 1) {
            throw new Error(
                    "[ERROR] Incorrect number of arguments provided. Received " + args.length + ", Expected 1.");
        }

        PeerInfo currentPeerInfo = new PeerInfo(Integer.parseInt(args[0]));
        System.out.println("Peer Process: " + args[0] + " NUM ARGUMENTS: " + args.length);

        currentPeerLogger = new Logger(currentPeerInfo.id);

        /* (Part 1) Reading Common Configuration File */
        File config = new File("./Common.cfg");

        Scanner configScanner = new Scanner(config);

        String configOutput[] = new String[6];

        for (int i = 0; i < 6; i++) {
            if (configScanner.hasNextLine()) {
                String line = configScanner.nextLine();
                int lastSpaceIndex = line.lastIndexOf(' ');
                // System.out.println(line);
                configOutput[i] = line.substring(lastSpaceIndex + 1);
            } else {
                configScanner.close();
                throw new Error("[ERROR] Common configuration file is missing entries.");
            }
        }

        configScanner.close();

        Configure commonConfiguration = new Configure(Integer.parseInt(configOutput[0]),
                Integer.parseInt(configOutput[1]),
                Integer.parseInt(configOutput[2]),
                configOutput[3],
                Integer.parseInt(configOutput[4]),
                Integer.parseInt(configOutput[5]));

        Integer filePieceCount = Math.ceilDiv(commonConfiguration.fileSize, commonConfiguration.pieceSize);

        myBitfield = new int[filePieceCount];
        /* End of Part 1 */

        /* (Part 2) Reading Peer Configuration File */
        int currentPeerID = currentPeerInfo.id;

        File peerInfoFile = new File("./PeerInfo.cfg");

        boolean inEarlierPeerSection = true;

        try (Scanner peerInfoScanner = new Scanner(peerInfoFile)) {
            while (peerInfoScanner.hasNextLine()) {
                String line = peerInfoScanner.nextLine();

                String[] peerInfo = line.split(" ");

                int newPeerID = Integer.parseInt(peerInfo[0]);

                if (currentPeerInfo.id == newPeerID) {
                    System.out.println("[INFO] Current peer found in peer config file.");

                    currentPeerInfo.hostname = peerInfo[1].equals("localhost") ? "127.0.0.1" : peerInfo[1];
                    currentPeerInfo.port = Integer.parseInt(peerInfo[2]);
                    currentPeerInfo.file = Integer.parseInt(peerInfo[3]);

                    inEarlierPeerSection = false;
                } else {
                    PeerInfo listPeer = new PeerInfo(newPeerID);
                    listPeer.hostname = peerInfo[1];
                    listPeer.port = Integer.parseInt(peerInfo[2]);
                    listPeer.file = Integer.parseInt(peerInfo[3]);

                    if (listPeer.file == 1) {
                        peersWithFullFile.add(listPeer.id);
                    }

                    // activePeers.put(newPeerID, listPeer);
                    if (inEarlierPeerSection)
                        earlierPeers.put(listPeer.id, listPeer);
                    allPeers.put(listPeer.id, listPeer);
                }
            }
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("[ERROR] Peer information file not found.\n");
        }

        if (currentPeerInfo.hostname.isEmpty()) {
            throw new Error("[ERROR] Invalid peer ID.");
        }

        // TODO: After marking bitfield we have to:
        // - properly parse file,
        // - create pieces,
        // - make storage for pieces,
        // - and reconstruct logic after all pieces are collected.
        // Update Bitfield if peer has the whole file.
        if (currentPeerInfo.file == 1) {
            System.out.println("[INFO] Peer " + currentPeerID + " have the full file.");
            for (int i = 0; i < myBitfield.length; i++) {
                myBitfield[i] = 1;
            }
        }

        // Just in case
        allPeers.put(currentPeerInfo.id, currentPeerInfo);

        /* End of Part 2 */

        // Start listener thread
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(currentPeerInfo.port)) {
                System.out.println("[" + currentPeerID + "] Listening on port " + currentPeerInfo.port);

                while (true) {
                    Socket socket = serverSocket.accept();
                    System.out.println("[INFO] Server-side connected.");

                    new Thread(() -> handleIncomingConnection(socket, currentPeerID, currentPeerLogger, allPeers))
                            .start();
                }
            } catch (IOException e) {
                System.err.println("Listener error: " + e.getMessage());
                System.exit(1);
            }
        }).start();

        // /* (Part 3) Start by sending handshake messaages to all active peers. */

        // Outbound Handshake Loop
        for (Map.Entry<Integer, PeerInfo> currActivePeer : earlierPeers.entrySet()) {
            System.out.println(
                    "[INFO] Attempting handshake with: " + currActivePeer.getValue().toString());

            Socket socket = new Socket(currActivePeer.getValue().hostname, currActivePeer.getValue().port);
            BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
            DataInputStream in = new DataInputStream(bis);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            currentPeerLogger.logOutboundTCPConnection(currActivePeer.getValue().id);

            sendHandshake(out, currentPeerID);

            int theirID = readHandshake(in);

            if (theirID != currActivePeer.getValue().id) {
                socket.close();
                throw new Exception("[ERROR] Intiated a connection with wrong peer. Attempt with peer #"
                        + currActivePeer.getValue().id + ". Instead got peer #" + theirID + ".");
            }

            System.out.println("[INFO] Outbound handshake completed with Peer #" + theirID + ".");

            ConnectionHandler handler = new ConnectionHandler(theirID, socket, in, out);

            connectionsByPeerID.put(theirID, handler);

            handler.sendBitfield();

            new Thread(handler).start();
        }

        /* End of part 3 */

        // TODO: Create an independent thread for periodically selecting preferred
        // neighbors and optimistically unchoking neighbors.

        Thread.currentThread().join();
    }
}
