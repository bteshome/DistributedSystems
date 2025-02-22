import { Component, inject, OnInit, signal } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { AuthService } from '../services/auth.service';

@Component({
  selector: 'app-signed-in',
  imports: [],
  templateUrl: './signed-in.component.html',
  styleUrl: './signed-in.component.css'
})

export class SignedInComponent implements OnInit {
  route = inject(ActivatedRoute)
  authService = inject(AuthService)

  ngOnInit(): void {
    this.route.queryParams.subscribe(params => {
      const code = params['code'];
      if (code) {
        this.authService.exchangeCodeForTokens(code);
      } else {
        console.warn('No authorization code received in redirect.');
      }
    });
  }
}
