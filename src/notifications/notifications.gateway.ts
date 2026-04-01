import { Logger } from '@nestjs/common';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';

@WebSocketGateway({
  cors: {
    origin: '*',
  },
})
export class NotificationsGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect
{
  private readonly logger = new Logger(NotificationsGateway.name);

  /** Map clientId → socket ID(s) */
  private clientSockets = new Map<string, Set<string>>();
  /** Map socket ID → clientId */
  private socketToClient = new Map<string, string>();

  @WebSocketServer()
  server: Server;

  afterInit() {
    this.logger.log('WebSocket gateway initialized');
  }

  handleConnection(client: Socket) {
    const clientId = client.handshake.query.clientId as string;

    if (!clientId) {
      this.logger.warn(
        `Client connected without clientId — disconnecting (socket: ${client.id})`,
      );
      client.disconnect(true);
      return;
    }

    // Track socket → clientId mapping
    this.socketToClient.set(client.id, clientId);

    // Track clientId → socket(s) mapping (one client can have multiple tabs)
    if (!this.clientSockets.has(clientId)) {
      this.clientSockets.set(clientId, new Set());
    }
    this.clientSockets.get(clientId)!.add(client.id);

    this.logger.log(
      `Client connected: ${clientId} (socket: ${client.id}, total sockets: ${this.clientSockets.get(clientId)!.size})`,
    );
  }

  handleDisconnect(client: Socket) {
    const clientId = this.socketToClient.get(client.id);

    if (clientId) {
      this.socketToClient.delete(client.id);

      const sockets = this.clientSockets.get(clientId);
      if (sockets) {
        sockets.delete(client.id);
        if (sockets.size === 0) {
          this.clientSockets.delete(clientId);
        }
      }

      this.logger.log(`Client disconnected: ${clientId} (socket: ${client.id})`);
    }
  }

  /**
   * Send an event to a specific client by their clientId.
   */
  notifyClient(clientId: string, event: string, data: any) {
    const socketIds = this.clientSockets.get(clientId);

    if (!socketIds || socketIds.size === 0) {
      this.logger.warn(
        `No active sockets for client "${clientId}" — message dropped (event: ${event})`,
      );
      return;
    }

    for (const socketId of socketIds) {
      this.server.to(socketId).emit(event, data);
    }

    this.logger.debug(
      `Sent "${event}" to client "${clientId}" (${socketIds.size} socket(s))`,
    );
  }
}
