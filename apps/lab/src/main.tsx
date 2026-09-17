import '@fraudwatch/ui/styles.css';
import { initTheme, ToastProvider } from '@fraudwatch/ui';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { App } from './App';
import { LabProvider } from './lab';

initTheme();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <ToastProvider>
        <LabProvider>
          <App />
        </LabProvider>
      </ToastProvider>
    </BrowserRouter>
  </StrictMode>,
);
