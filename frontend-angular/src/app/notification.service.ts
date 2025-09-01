import { Injectable } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class NotificationService {
  private socket: WebSocket;

  constructor() {
    this.socket = new WebSocket('ws://localhost:3000/ws?userId=u1');
  }

  onMessage(callback: (msg: any) => void) {
    this.socket.onmessage = (event) => {
      callback(JSON.parse(event.data));
    };
  }
}
