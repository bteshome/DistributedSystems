import { Routes } from '@angular/router';

export const routes: Routes = [
    {
        path: '',
        pathMatch: 'full',
        loadComponent: () => {
            return import('./products/products.component')
                .then((m) => m.ProductsComponent);
        }
    },
    {
        path: 'orders',
        pathMatch: 'full',
        loadComponent: () => {
            return import('./order-list/order-list.component')
                .then((m) => m.OrderListComponent);
        }
    },
    {
        path: 'orders-place',
        pathMatch: 'full',
        loadComponent: () => {
            return import('./order-create/order-create.component')
                .then((m) => m.OrderCreateComponent);
        }
    },
    {
        path: 'signedin',
        pathMatch: 'full',
        loadComponent: () => {
            return import('./signed-in/signed-in.component')
                .then((m) => m.SignedInComponent);
        }
    }
];
