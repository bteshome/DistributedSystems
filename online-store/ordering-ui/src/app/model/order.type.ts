import { LineItem } from "./lineItem.type"

export type Order = {
    orderNumber: string,
    lineItems: LineItem[]
}