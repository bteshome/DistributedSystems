import { Component, inject, signal } from '@angular/core';
import { RouterLink } from '@angular/router';
import { AuthService } from '../../services/auth.service';

@Component({
  selector: 'app-header',
  imports: [RouterLink],
  templateUrl: './header.component.html',
  styleUrl: './header.component.css'
})

export class HeaderComponent {
  applicationName = signal('Online Store');
  authService = inject(AuthService)  
  user = this.authService.user;

  signin() : void {
    this.authService.signin();
  }

  signout() : void {
    this.authService.signout();
  }
}
