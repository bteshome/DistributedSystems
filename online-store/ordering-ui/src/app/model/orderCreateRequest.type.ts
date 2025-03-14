import { CartItem } from "./cartItem.type"

export type OrderCreateRequest = {
  username: string,
  firstName: string,
  lastName: string,
  email: string,
  lineItems: CartItem[]
}
