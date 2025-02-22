import { Component, OnInit } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { SidebarComponent } from './components/sidebar/sidebar.component';
import { HeaderComponent } from './components/header/header.component';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, HeaderComponent, SidebarComponent],
  template: `
    <app-header />
    <div class="container-fluid">
      <div class="row">
        <app-sidebar />
        <main class="col-md-9 ms-sm-auto col-lg-10 px-md-4">
          <router-outlet />
        </main>
      </div>
    </div>
  `
})

export class AppComponent implements OnInit {
  title = 'Online Store';
  
  ngOnInit(): void { }
}

