import { inject, Injectable, signal } from '@angular/core';
import { TokeResponse } from '../model/TokenResponse.type';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { User } from '../model/user.type';
import { Router } from '@angular/router';
import { environment } from '../../environments/environment';

@Injectable({
  providedIn: 'root'
})

export class AuthService {
  baseUrl = environment.authBaseUrl;
  authEndpoint = this.baseUrl + "/realms/apigateway/protocol/openid-connect/auth";
  tokenEndpoint = this.baseUrl + "/realms/apigateway/protocol/openid-connect/token"
  userInfoEndpoint = this.baseUrl + "/realms/apigateway/protocol/openid-connect/userinfo"
  clientId = environment.clientId;
  clientSecret = environment.clientSecret;
  redirectUri = window.location.origin + "/signedin";
  http = inject(HttpClient)
  router = inject(Router);
  tokenResponse = signal<TokeResponse | null>(null);
  user = signal<User | null>(null);

  signin() {
    const redirectUriEncoced = encodeURIComponent(this.redirectUri);
    const authUrl = `${this.authEndpoint}?response_type=code&redirect_uri=${redirectUriEncoced}&client_id=${this.clientId}&client_secret=${this.clientSecret}&scope=openid`;
    window.location.href = authUrl;
  }

  exchangeCodeForTokens(code: string): void {
    const headers = new HttpHeaders({
      'Content-Type': 'application/x-www-form-urlencoded'
    });

    const body = new URLSearchParams();
    body.set('client_id', this.clientId);
    body.set('client_secret', this.clientSecret);
    body.set('grant_type', 'authorization_code');
    body.set('code', code);
    body.set('redirect_uri', this.redirectUri);
    body.set('redirect_uri', this.redirectUri);

    this.http.post<TokeResponse>(this.tokenEndpoint, body.toString(), { headers })
      .subscribe((response: TokeResponse) => {
        this.tokenResponse.set(response);
        this.getUserInfo(response.access_token);
        this.router.navigate(['/']);
      });
  }

  getUserInfo(token : string) {
    const headers = new HttpHeaders ({
      'Authorization': 'Bearer ' + token
    });

    this.http.get<User | null>(this.userInfoEndpoint, { headers })
      .subscribe((user : User | null) => {
        this.user.set(user);
      });
  }

  signout() : void {
    this.tokenResponse.set(null);
    this.user.set(null);
    this.router.navigate(['/']);
  }
}
