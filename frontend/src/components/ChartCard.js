import React from 'react';
import { Card, CardContent, Typography } from '@mui/material';

function ChartCard({ title, children }) {
  return (
    <Card elevation={3} sx={{ width: '100%' }}>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          {title}
        </Typography>
        <div style={{ width: '100%' }}>{children}</div> {/* Ensure the content spans full width */}
      </CardContent>
    </Card>
  );
}

export default ChartCard;
