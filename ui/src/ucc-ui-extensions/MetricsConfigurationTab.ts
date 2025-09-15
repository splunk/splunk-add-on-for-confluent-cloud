// ui/src/ucc-ui-extensions/MetricsConfigurationTab.ts
import React from "react";
import ReactDOM from "react-dom";
import { CustomTabBase } from "@splunk/add-on-ucc-framework";

// Lazy load the main component for better performance
const MetricsConfiguration = React.lazy(() => import("./components/MetricsConfiguration"));

/**
 * UCC Custom Tab wrapper for Metrics Configuration
 * This component integrates MetricsConfiguration.tsx into the UCC framework
 * as a custom tab in the Splunk add-on configuration interface.
 */
export default class MetricsConfigurationTab extends CustomTabBase {
  /**
   * Render the metrics configuration component
   * This method is called by UCC framework when the tab is displayed
   */
  render() {
    // Create a fallback loading component
    const LoadingFallback = React.createElement('div', {
      style: {
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '200px',
        fontSize: '14px',
        color: '#666'
      }
    }, 'Loading metrics configuration...');

    // Render the lazy-loaded component with Suspense
    ReactDOM.render(
      React.createElement(
        React.Suspense,
        { fallback: LoadingFallback },
        React.createElement(MetricsConfiguration)
      ),
      this.el
    );
  }

  /**
   * Cleanup when component unmounts
   * This ensures proper cleanup when user navigates away from the tab
   */
  unmount() {
    if (this.el) {
      ReactDOM.unmountComponentAtNode(this.el);
    }
  }
}