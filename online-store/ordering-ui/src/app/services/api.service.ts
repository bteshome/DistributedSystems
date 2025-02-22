import { HttpClient, HttpHeaders } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Product } from '../model/product.type';
import { Order } from '../model/order.type';
import { OrderCreateRequest } from '../model/orderCreateRequest.type';
import { OrderCreateResponse } from '../model/orderCreateResponse.type';
import { environment } from '../../environments/environment';

@Injectable({
  providedIn: 'root'
})

export class ApiService {
  baseUrl = environment.apiBaseUrl;
  productsUrl: string = this.baseUrl + "/inventory/products/"
  ordersUrl: string = this.baseUrl + "/orders/"
  http = inject(HttpClient);

  getProducts() {
    return this.http.get<Array<Product>>(this.productsUrl)
  }

  getOrders(email: string) {
    return this.http.get<Array<Order>>(this.ordersUrl + "?email=" + email)
  }

  createOrder(order: OrderCreateRequest) {
    const headers = new HttpHeaders({
      'Content-Type': 'application/json'
    });

    return this.http.post<OrderCreateResponse>(this.ordersUrl, order, { headers });
  }
}
