import { HttpClient, HttpHeaders } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Product } from '../model/product.type';
import { Order } from '../model/order.type';
import { OrderCreateRequest } from '../model/orderCreateRequest.type';
import { OrderCreateResponse } from '../model/orderCreateResponse.type';
import { environment } from '../../environments/environment';
import { AuthService } from './auth.service';

@Injectable({
  providedIn: 'root'
})

export class ApiService {
  baseUrl = environment.apiBaseUrl;
  productsUrl: string = this.baseUrl + "/inventory/products/"
  ordersQueryUrl: string = this.baseUrl + "/orders/query/"
  ordersCreateUrl: string = this.baseUrl + "/orders/create/"
  http = inject(HttpClient);
  authService = inject(AuthService);

  createAuthHeader() {
    if (!this.authService.tokenResponse())
      this.authService.signin();

    const headers = new HttpHeaders ({
      'Authorization': 'Bearer ' + this.authService.tokenResponse()?.access_token
    });

    return headers;
  }

  getProducts() {
    return this.http.get<Array<Product>>(this.productsUrl)
  }

  getOrders(email: string) {
    const headers = this.createAuthHeader();
    return this.http.get<Array<Order>>(this.ordersQueryUrl + "?email=" + email, { headers })
  }

  createOrder(order: OrderCreateRequest) {
    const headers = this.createAuthHeader();
    return this.http.post<OrderCreateResponse>(this.ordersCreateUrl, order, { headers });
  }
}
