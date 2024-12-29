import React from 'react';
import { AppBar, Toolbar, Typography, Box } from '@mui/material';

const Header = () => {
  return (
    <AppBar position="static">
      <Typography variant="h6" style={{ flexGrow: 1 }}>
        Sentiment Analysis Dashboard
      </Typography>
    </AppBar>
  );
};

export default Header;
