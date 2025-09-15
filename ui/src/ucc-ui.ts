// ui/src/ucc-ui.ts
import { uccInit } from "@splunk/add-on-ucc-framework";
import MetricsConfigurationTabClass from "./ucc-ui-extensions/MetricsConfigurationTab";

uccInit({
  MetricsConfigurationTab: {
    component: MetricsConfigurationTabClass,
    type: 'tab',
  },
}).catch((error) => {
  console.error("Could not load UCC", error);
});