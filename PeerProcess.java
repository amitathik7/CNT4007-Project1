import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Clock;

public class PeerProcess {

    static final String HEADER = "P2PFILESHARINGPROJ";

    // Tracks the current file pieces that neighbors own.
    static Map<Integer, int[]> neighborBitfields = Collections.synchronizedMap(new HashMap<>());
    static Map<Integer, Integer> bytesDownloadedFromPeers = Collections.synchronizedMap(new HashMap<>());

    // Tracks information on peers (requestedPieces is a set of neighbors from whom
    // we have requested pieces in the current cycle).
    static Set<Integer> interestedPeers = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> peersChokingMe = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> peersIAmChoking = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> peersWithFullFile = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> preferredNeighbors = Collections.synchronizedSet(new HashSet<>());
    static Set<Integer> requestedPieces = Collections.synchronizedSet(new HashSet<>());

    // Stores TCP connections so we don't reopen connections already existing with
    // neighbors.
    static Map<Integer, ConnectionHandler> connectionsByPeerID = Collections.synchronizedMap(new HashMap<>());

    static Integer optimisticUnchockedPeer;

    // Tracks our file pieces we own.
    static int[] myBitfield;

    static Logger currentPeerLogger;

    static PeerInfo currentPeerInfo;

    static Configure commonConfiguration;

    static int checkingDelaySeconds = 1;

    static class Configure {
        int numPreferredNeighbors;
        int unchokingInterval;
        int optimisticUnchokingInterval;
        String fileName;
        long fileSize;
        int pieceSize;

        Configure(int n, int ui, int oui, String name, long fs, int ps) {
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

        void logDownloadedPiece(int neighborID, int pieceIndex, int pieceCount) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId
                        + " has downloaded the piece " + pieceIndex + " from " + neighborID
                        + ". Now the number of pieces it has is " + pieceCount + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }

        void logHaveMessage(int neighborID, int pieceIndex) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId
                        + " received the 'have' message from " + neighborID + " for the piece " + pieceIndex + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }

        void logFullDownload() {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(
                        this.loggerClock.instant() + ": Peer " + this.peerId + " has downloaded the complete file.\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }

        void logChangePreferredNeighbors(Set<Integer> neighbors) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                List<Integer> sorted = new ArrayList<>(neighbors);
                Collections.sort(sorted);

                String neighborList = sorted.toString().replace("[", "").replace("]", "");

                logWriter.write(this.loggerClock.instant() + ": Peer " + this.peerId + " has the preferred neighbors "
                        + neighborList + ".\n");
            } catch (IOException e) {
                throw new Error("Unable to edit log." + e.getMessage());
            }
        }

        void logOptimisticallyUnchokedNeighbor(Integer neighbor) {
            try (FileWriter logWriter = new FileWriter(this.logFile, true)) {
                logWriter.write(this.loggerClock.instant() + ": Peer " + peerId
                        + " has the optimistically unchoked neighbor " + neighbor + ".\n");
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

    // This map is for creating a new connection with all already existing
    // neighbors.
    static Map<Integer, PeerInfo> earlierPeers = new HashMap<>();
    // This map is for handling the incoming connections.
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

        // PeerID value from handshake header.
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

    static byte[] readPieceFromDisk(int peerID, int pieceIndex) throws IOException {
        File pieceFile = getPieceFile(peerID, pieceIndex);

        if (!pieceFile.exists()) {
            throw new IOException("[ERROR]: Piece " + pieceIndex + " requested doesn't exist for peer " + peerID + ".");
        }

        return Files.readAllBytes(pieceFile.toPath());
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
            peersIAmChoking.add(theirID);

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

    static Integer choosePieceToRequestFrom(int peerID) {
        int[] neighborBitField = neighborBitfields.get(peerID);

        if (neighborBitField == null) {
            return null;
        }

        List<Integer> possibleCandidates = new ArrayList<>();

        for (int i = 0; i < neighborBitField.length; i++) {
            if (neighborBitField[i] == 1 && myBitfield[i] == 0 && !requestedPieces.contains(i)) {
                possibleCandidates.add(i);
            }
        }

        if (possibleCandidates.isEmpty()) {
            return null;
        }

        int chosenCandidate = possibleCandidates.get(new Random().nextInt(possibleCandidates.size()));

        requestedPieces.add(chosenCandidate);

        return chosenCandidate;
    }

    static boolean isBitfieldComplete(int[] bitfield) {
        for (int i : bitfield) {
            if (i == 0) {
                return false;
            }
        }

        return true;
    }

    static int getDownloadedBytesFromPeer(int peerID) {
        return bytesDownloadedFromPeers.getOrDefault(peerID, 0);
    }

    static void resetDownloadRates() {
        bytesDownloadedFromPeers.clear();
    }

    static List<Integer> getInterestedPeersSortedByDownloadRate() {
        List<Integer> peers = new ArrayList<>(interestedPeers);
        Collections.shuffle(peers);

        peers.sort((a, b) -> Integer.compare(getDownloadedBytesFromPeer(b), getDownloadedBytesFromPeer(a)));

        return peers;
    }

    static Set<Integer> selectPreferredNeighbors() {
        List<Integer> candidates = new ArrayList<>(interestedPeers);

        if (candidates.isEmpty()) {
            return new HashSet<>();
        }

        if (hasCompleteFile()) {
            Collections.shuffle(candidates);
        } else {
            candidates = getInterestedPeersSortedByDownloadRate();
        }

        Set<Integer> newPreferred = new HashSet<>();
        int limit = Math.min(commonConfiguration.numPreferredNeighbors, candidates.size());

        for (int i = 0; i < limit; i++) {
            newPreferred.add(candidates.get(i));
        }

        return newPreferred;
    }

    static Integer optimisticUnchokeNeighbor() {
        List<Integer> candidates = interestedPeers.stream().distinct().filter(peersIAmChoking::contains)
                .collect(Collectors.toList());

        Collections.shuffle(candidates);

        return candidates.size() != 0 ? candidates.get(0) : null;
    }

    static void applyPreferredNeighborSelection(Set<Integer> newPreferred) {
        Set<Integer> oldPreferred = new HashSet<>(preferredNeighbors);

        for (Integer peerID : newPreferred) {
            if (!oldPreferred.contains(peerID)) {
                try {
                    ConnectionHandler handler = connectionsByPeerID.get(peerID);

                    if (handler != null) {
                        handler.sendMessage(1, null);
                    }
                    peersIAmChoking.remove(peerID);
                } catch (IOException e) {
                    System.out.println("[ERROR]: Unable to send the unchoke message to the peer " + peerID);
                }
            }
        }

        for (Integer peerID : oldPreferred) {
            if (!newPreferred.contains(peerID)
                    && (optimisticUnchockedPeer == null || !peerID.equals(optimisticUnchockedPeer))) {
                try {
                    ConnectionHandler handler = connectionsByPeerID.get(peerID);

                    if (handler != null) {
                        handler.sendMessage(0, null);
                    }

                    peersIAmChoking.add(peerID);
                } catch (IOException e) {
                    System.out.println("[ERROR]: Failed to send the choke message to peer " + peerID);
                }
            }
        }

        boolean changed = !oldPreferred.equals(newPreferred);

        preferredNeighbors.clear();
        preferredNeighbors.addAll(newPreferred);

        if (changed) {
            currentPeerLogger.logChangePreferredNeighbors(preferredNeighbors);
        }
    }

    static void applyUnchokedNeighborSelection(Integer unchokedNeighbor) {
        try {
            ConnectionHandler handler = connectionsByPeerID.get(unchokedNeighbor);

            if (handler != null) {
                handler.sendMessage(1, null);
            }

            peersIAmChoking.remove(unchokedNeighbor);

            optimisticUnchockedPeer = unchokedNeighbor;
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to send the unchoking message to the optimistically unchoked neighbor "
                    + unchokedNeighbor + ".");
        }

        currentPeerLogger.logOptimisticallyUnchokedNeighbor(unchokedNeighbor);
    }

    static void startPreferredNeighborSelectionThread() {
        Thread preferredNeighborThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(commonConfiguration.unchokingInterval * 1000);

                    Set<Integer> newPreferred = selectPreferredNeighbors();

                    applyPreferredNeighborSelection(newPreferred);

                    resetDownloadRates();
                } catch (InterruptedException e) {
                    System.out.println("[ERROR]: Preffered neighbor selection was interrupted.");
                } catch (Exception e) {
                    System.out.println(
                            "[ERROR]: Preferred neighbor selection stopped because of error: " + e.getMessage());
                }
            }
        });

        preferredNeighborThread.start();
    }

    static void startOptimisticUnchokingThread() {
        Thread optimisticUnchokingThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(commonConfiguration.optimisticUnchokingInterval * 1000);

                    Integer unchokedNeighbor = optimisticUnchokeNeighbor();

                    if (optimisticUnchockedPeer != null && !preferredNeighbors.contains(optimisticUnchockedPeer)
                            && !optimisticUnchockedPeer.equals(unchokedNeighbor)) {

                        try {
                            ConnectionHandler handler = connectionsByPeerID.get(optimisticUnchockedPeer);

                            if (handler != null) {
                                handler.sendMessage(0, null);
                            }

                            peersIAmChoking.add(optimisticUnchockedPeer);
                        } catch (IOException e) {
                            System.out.println(
                                    "[ERROR]: The previously optimistically unchoked neighbor couldn't be choked."
                                            + e.getMessage());
                        }
                    }

                    if (unchokedNeighbor != null) {
                        applyUnchokedNeighborSelection(unchokedNeighbor);
                    } else {
                        optimisticUnchockedPeer = null;
                    }
                } catch (InterruptedException e) {
                    System.out.println("[ERROR]: Optimistic unchoking selection thread was interrupted.");
                } catch (Exception e) {
                    System.out.println("[ERROR]: Optimistic unchoking selection thread was stopped because of an error."
                            + e.getMessage());
                }
            }
        });

        optimisticUnchokingThread.start();
    }

    static void startCompletionCheckMonitor() {
        Thread completionThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(checkingDelaySeconds * 1000);

                    if (haveAllPeersCompleted()) {
                        System.out.println("[INFO]: This peer " + currentPeerInfo.id
                                + " has detected all peers are completed. Closing all connections now.");

                        closeAllConnections();
                        System.exit(0);
                    }
                } catch (InterruptedException e) {
                    System.out.println(
                            "[ERROR]: The completion check thread has been closed. Can't close automatically now."
                                    + e.getMessage());
                } catch (Exception e) {
                    System.out.println("[ERROR]: Completion checking thread has been closed because of an exception: "
                            + e.getMessage());
                }
            }
        });

        completionThread.start();
    }

    static Boolean haveAllPeersCompleted() {
        return peersWithFullFile.size() == allPeers.size();
    }

    static void closeAllConnections() {
        for (ConnectionHandler handler : connectionsByPeerID.values()) {
            try {
                if (handler.in != null) {
                    handler.in.close();
                }
            } catch (IOException _) {

            }

            try {
                if (handler.out != null) {
                    handler.out.close();
                }
            } catch (IOException _) {

            }

            try {
                if (handler.socket != null && !handler.socket.isClosed()) {
                    handler.socket.close();
                }
            } catch (IOException _) {

            }
        }
    }

    static void processMessage(Message msg, int peerID) throws IOException {
        switch (msg.type) {
            case 0:
                peersChokingMe.add(peerID);
                currentPeerLogger.logChokedByPeer(peerID);
                break;
            case 1:
                peersChokingMe.remove(peerID);
                currentPeerLogger.logUnchokedByPeer(peerID);

                Integer possiblePiece = choosePieceToRequestFrom(peerID);

                if (possiblePiece != null) {
                    ByteBuffer payload = ByteBuffer.allocate(4);

                    payload.putInt(possiblePiece);

                    connectionsByPeerID.get(peerID).sendMessage(6, payload.array());
                }

                break;
            case 2:
                interestedPeers.add(peerID);
                currentPeerLogger.logInterestedPeer(peerID);
                break;
            case 3:
                interestedPeers.remove(peerID);
                currentPeerLogger.logUninterestedPeer(peerID);
                break;
            case 4: {
                int pieceIndex = ByteBuffer.wrap(msg.messagePayload).getInt();

                int[] neighborBitfield = neighborBitfields.get(peerID);

                if (neighborBitfield == null) {
                    neighborBitfield = new int[myBitfield.length];
                    neighborBitfields.put(peerID, neighborBitfield);
                }

                neighborBitfields.get(peerID)[pieceIndex] = 1;

                if (isBitfieldComplete(neighborBitfields.get(peerID))) {
                    peersWithFullFile.add(peerID);
                }

                if (isInterested(neighborBitfields.get(peerID))) {
                    connectionsByPeerID.get(peerID).sendMessage(2, null);
                } else {
                    connectionsByPeerID.get(peerID).sendMessage(3, null);
                }

                currentPeerLogger.logHaveMessage(peerID, pieceIndex);
                break;
            }
            case 5:
                int[] receivedBitfield = payloadToBitfield(msg.messagePayload);
                neighborBitfields.put(peerID, receivedBitfield);

                if (isBitfieldComplete(receivedBitfield)) {
                    peersWithFullFile.add(peerID);
                }

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
            case 6: {
                int pieceIndex = ByteBuffer.wrap(msg.messagePayload).getInt();

                if (pieceIndex < 0 || pieceIndex > (myBitfield.length - 1)) {
                    System.out.println("[ERROR]: Invalid piece index " + pieceIndex + ".");
                    break;
                }

                if (peersIAmChoking.contains(peerID)) {
                    System.out.println("[INFO]: Ignoring request from " + peerID + " because I am blocking them.");
                    break;
                }

                if (myBitfield[pieceIndex] == 0) {
                    System.out.println("[INFO]: Ignoring request from " + peerID + " because I am don't have piece "
                            + pieceIndex + ".");
                    break;
                }

                try {
                    byte[] pieceData = readPieceFromDisk(currentPeerInfo.id, pieceIndex);

                    ByteBuffer payload = ByteBuffer.allocate(4 + pieceData.length);
                    payload.putInt(pieceIndex);
                    payload.put(pieceData);

                    connectionsByPeerID.get(peerID).sendMessage(7, payload.array());
                } catch (IOException e) {
                    System.out.println("[ERROR]: Failed to send " + pieceIndex + ".");
                }

                break;
            }
            case 7: {
                ByteBuffer pieceBuffer = ByteBuffer.wrap(msg.messagePayload);
                int pieceIndex = pieceBuffer.getInt();

                byte[] pieceData = new byte[msg.messagePayload.length - 4];
                pieceBuffer.get(pieceData);

                if (myBitfield[pieceIndex] == 0) {
                    bytesDownloadedFromPeers.merge(peerID, pieceData.length, Integer::sum);

                    try {
                        savePieceToDisk(currentPeerInfo.id, pieceIndex, pieceData);
                    } catch (IOException e) {
                        throw new Error("[ERROR]: Error occurred while saving piece " + pieceIndex + " to disk.");
                    }

                    myBitfield[pieceIndex] = 1;
                    requestedPieces.remove(pieceIndex);

                    if (hasCompleteFile()) {
                        try {
                            reconstructFile(currentPeerInfo.id, commonConfiguration.fileName, myBitfield.length);
                        } catch (IOException e) {
                            throw new Error("[ERROR]: Error occurred while reconstructing full file.");
                        }
                        peersWithFullFile.add(currentPeerInfo.id);

                        currentPeerLogger.logFullDownload();
                    }

                    int totalPieces = 0;

                    for (int i : myBitfield) {
                        totalPieces += i;
                    }

                    currentPeerLogger.logDownloadedPiece(peerID, pieceIndex, totalPieces);

                    for (Map.Entry<Integer, ConnectionHandler> i : connectionsByPeerID.entrySet()) {
                        ConnectionHandler currConnection = i.getValue();

                        ByteBuffer payload = ByteBuffer.allocate(4);
                        payload.putInt(pieceIndex);

                        currConnection.sendMessage(4, payload.array());
                    }

                    for (Map.Entry<Integer, int[]> i : neighborBitfields.entrySet()) {
                        int[] neighborBitfield = i.getValue();
                        int neighborID = i.getKey();

                        if (isInterested(neighborBitfield)) {
                            connectionsByPeerID.get(neighborID).sendMessage(2, null);
                        } else {
                            connectionsByPeerID.get(neighborID).sendMessage(3, null);
                        }
                    }
                }

                if (!hasCompleteFile() && !peersChokingMe.contains(peerID)) {
                    Integer nextPiece = choosePieceToRequestFrom(peerID);
                    if (nextPiece != null) {
                        try {
                            ByteBuffer requestPayload = ByteBuffer.allocate(4);
                            requestPayload.putInt(nextPiece);
                            connectionsByPeerID.get(peerID).sendMessage(6, requestPayload.array());
                        } catch (IOException e) {
                            System.out.println("[ERROR]: Unable to send next piece request message." + e.getMessage());
                        }
                    }
                }

                break;
            }
            default:
                throw new Error("[ERROR] Received improper message: " + msg.toString());
        }
    }

    // Smaller Helper Functions START

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

    static boolean isInterested(int[] otherBitfield) {
        if (otherBitfield.length != myBitfield.length) {
            throw new Error("[ERROR] Two peers have different bitfield sizes.");
        }

        for (int i = 0; i < myBitfield.length; i++) {
            if (myBitfield[i] == 0 && otherBitfield[i] == 1) {
                return true;
            }
        }

        return false;
    }

    static File getSplitFile(String largeFileName, byte[] buffer, int length, String splitFileDirPath)
            throws IOException {
        File splitFile = File.createTempFile(largeFileName + "-", "-split", new File(splitFileDirPath));

        try (FileOutputStream fos = new FileOutputStream(splitFile)) {
            fos.write(buffer, 0, length);
        }

        return splitFile;
    }

    static File getPieceFile(int peerID, int pieceIndex) {
        return new File(peerID + "/split/piece_" + pieceIndex);
    }

    static void savePieceToDisk(int peerID, int pieceIndex, byte[] pieceData) throws IOException {
        File pieceFile = getPieceFile(peerID, pieceIndex);
        try (FileOutputStream fos = new FileOutputStream(pieceFile)) {
            fos.write(pieceData);
        }
    }

    static boolean hasCompleteFile() {
        for (int bit : myBitfield) {
            if (bit == 0) {
                return false;
            }
        }

        return true;
    }

    static void reconstructFile(int peerID, String outputFileName, int totalPieces) throws IOException {
        File outputFile = new File(peerID + "/" + outputFileName);

        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            for (int i = 0; i < totalPieces; i++) {
                File pieceFile = getPieceFile(peerID, i);

                if (!pieceFile.exists()) {
                    throw new IOException("[ERROR]: Missing the piece file for chunk " + i);
                }

                try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(pieceFile))) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;

                    while ((bytesRead = bis.read(buffer)) != -1) {
                        bos.write(buffer, 0, bytesRead);
                    }
                }
            }
        }
    }

    // Smaller Helper Functions END

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new Error(
                    "[ERROR] Incorrect number of arguments provided. Received " + args.length + ", Expected 1.");
        }

        currentPeerInfo = new PeerInfo(Integer.parseInt(args[0]));
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
                configOutput[i] = line.substring(lastSpaceIndex + 1);
            } else {
                configScanner.close();
                throw new Error("[ERROR] Common configuration file is missing entries.");
            }
        }

        configScanner.close();

        commonConfiguration = new Configure(Integer.parseInt(configOutput[0]),
                Integer.parseInt(configOutput[1]),
                Integer.parseInt(configOutput[2]),
                configOutput[3],
                Long.parseLong(configOutput[4]),
                Integer.parseInt(configOutput[5]));

        Integer filePieceCount = (int) Math.ceilDiv(commonConfiguration.fileSize, (long) commonConfiguration.pieceSize);

        myBitfield = new int[filePieceCount];

        /* Reading Peer Configuration File */
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

                    if (currentPeerInfo.file == 1) {
                        peersWithFullFile.add(currentPeerInfo.id);
                    }
                } else {
                    PeerInfo listPeer = new PeerInfo(newPeerID);
                    listPeer.hostname = peerInfo[1];
                    listPeer.port = Integer.parseInt(peerInfo[2]);
                    listPeer.file = Integer.parseInt(peerInfo[3]);

                    System.out.println(listPeer);

                    if (listPeer.file == 1) {
                        peersWithFullFile.add(listPeer.id);
                    }

                    if (inEarlierPeerSection) {
                        earlierPeers.put(listPeer.id, listPeer);
                    }
                    allPeers.put(listPeer.id, listPeer);
                }
            }
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("[ERROR] Peer information file not found.\n");
        }

        if (currentPeerInfo.hostname.isEmpty()) {
            throw new Error("[ERROR] Invalid peer ID.");
        }

        String splitFilePath = currentPeerID + "/split/";
        File splitFileDirectory = new File(splitFilePath);
        boolean splitFileDirectoryCreated = splitFileDirectory.mkdir();

        if (!splitFileDirectory.exists() && !splitFileDirectoryCreated) {
            System.out.println("[ERROR]: Unable to make split file directory.");
        }

        if (currentPeerInfo.file == 1) {
            String filepath = currentPeerID + "/" + commonConfiguration.fileName;
            File f = new File(filepath);

            if (!f.exists() || f.isDirectory()) {
                throw new Error("[ERROR] Peer " + currentPeerID + " should have full file but it can't find "
                        + commonConfiguration.fileName + " in it's directory.");
            }

            System.out.println("[INFO] Peer " + currentPeerID + " have the full file.");
            for (int i = 0; i < myBitfield.length; i++) {
                myBitfield[i] = 1;
            }

            try (InputStream in = Files.newInputStream(f.toPath())) {
                byte[] buffer = new byte[commonConfiguration.pieceSize];
                int bytesRead = 0;
                int piecesIndex = 0;

                while ((bytesRead = in.read(buffer)) != -1) {
                    byte[] exactPiece = Arrays.copyOf(buffer, bytesRead);
                    savePieceToDisk(currentPeerID, piecesIndex, exactPiece);
                    piecesIndex += 1;
                }
            }
        }

        // Just in case
        allPeers.put(currentPeerInfo.id, currentPeerInfo);

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

        /* Sending handshake messaages to all active peers. */

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

        peersIAmChoking.addAll(connectionsByPeerID.keySet());
        startPreferredNeighborSelectionThread();

        startOptimisticUnchokingThread();

        startCompletionCheckMonitor();

        Thread.currentThread().join();
    }
}
