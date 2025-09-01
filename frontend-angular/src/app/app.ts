import { Component, OnInit, NgZone } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-root',
  imports: [CommonModule],
  templateUrl: './app.html',
  styleUrls: ['./app.css']
})
export class AppComponent implements OnInit {
  notifications: string[] = [];
  ws!: WebSocket;

  constructor(private zone: NgZone) {}

  ngOnInit() {
    this.ws = new WebSocket("ws://localhost:3000/ws?userId=u1");

    this.ws.onopen = () => {
      console.log("✅ WebSocket connected");
    };

    this.ws.onmessage = (msg) => {
      console.log("📩 WS message:", msg.data);
      const parsed = JSON.parse(msg.data);

      if (parsed.type === "in-app") {
        const payload = parsed.payload;

        // Run inside Angular zone so UI updates
        this.zone.run(() => {
          this.notifications.push(payload.body);
        });
      }
    };
  }
}