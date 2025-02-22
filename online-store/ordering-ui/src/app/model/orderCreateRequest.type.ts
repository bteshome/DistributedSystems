import { CartItem } from "./cartItem.type"

export type OrderCreateRequest = {
    email: string,
    lineItems: CartItem[]
}
