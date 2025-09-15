// ui/src/types/ucc-framework.d.ts
declare module "@splunk/add-on-ucc-framework" {
  export class CustomTabBase {
    el: HTMLElement;
    render(): void;
    unmount?(): void;
  }
  
  export class CustomControlBase {
    el: HTMLElement;
    data: any;
    setValue(value: any): void;
    render(): void;
  }
  
  export function uccInit(config: Record<string, any>): Promise<void>;
}