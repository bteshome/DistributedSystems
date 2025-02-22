export class CartItem {
    constructor(
        public name: string,
        public skuCode: string,
        public price: number,
        public quantity: number
    ) {}

    toString(): string {
        return `${this.name}-${this.skuCode}-${this.price}-${this.quantity}`;
    }

    static fromString(value: string) : CartItem {
        let valuesSplitted = value.split("-");
        return new CartItem(valuesSplitted[0], valuesSplitted[1], Number(valuesSplitted[2]), Number(valuesSplitted[3]));
    }
}
