import { Component, inject, OnInit, signal } from '@angular/core';
import { ApiService } from '../services/api.service';
import { Order } from '../model/order.type';
import { catchError } from 'rxjs';
import { AuthService } from '../services/auth.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-order-list',
  imports: [],
  templateUrl: './order-list.component.html',
  styleUrl: './order-list.component.css'
})

export class OrderListComponent implements OnInit {
  page = signal('My Orders');
  apiService = inject(ApiService);
  authService = inject(AuthService);
  user = this.authService.user;
  orders = signal<Array<Order>>([])
  loading = signal<boolean>(false);
  errorMessage = signal<string>("");

  ngOnInit(): void {
    let userObject = this.user();

    if (!userObject) {
      this.authService.signin();
    } else {
      this.loading.set(true);
      this.errorMessage.set("");
      this.orders.set([]);

      this.apiService.getOrders(userObject.preferred_username)
        .pipe(
          catchError(error => {
            this.loading.set(false);
            this.errorMessage.set(error.error.error);
            return [];
          })
        )
        .subscribe((data) => {
          this.loading.set(false);
          this.orders.set(data);
        });
    }
  }
}
