import React from 'react';
import { Drawer, List, ListItem, ListItemText, Typography } from '@mui/material';

function Sidebar() {
  return (
    <Drawer
      variant="permanent"
      sx={{
        width: 240,
        flexShrink: 0,
        [`& .MuiDrawer-paper`]: { width: 240, boxSizing: 'border-box' },
      }}
    >
      <List>
        <ListItem>
          <Typography variant="h6">Dashboard</Typography>
        </ListItem>
        <ListItem button>
          <ListItemText primary="Overview" />
        </ListItem>
        <ListItem button>
          <ListItemText primary="Reports" />
        </ListItem>
        <ListItem button>
          <ListItemText primary="Settings" />
        </ListItem>
      </List>
    </Drawer>
  );
}

export default Sidebar;
