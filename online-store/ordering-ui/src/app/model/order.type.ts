import { LineItem } from "./lineItem.type"

export type Order = {
  orderNumber: string,
  orderDatetime: Date,
  status: string,
  lineItems: LineItem[]
}
