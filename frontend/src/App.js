// App.js
import './App.css'
import MainDash from './components/MainDash/MainDash';
import RightSide from './components/RigtSide/RightSide';
import Sidebar from './components/Sidebar';
import { Routes, Route } from 'react-router-dom';

function App() {
  return (
    <div className="App">
      <div className="AppGlass">
        {/* The Sidebar is outside the <Routes> 
            so it stays visible no matter the route */}
        <Sidebar />

        {/* The "right side" can be rendered on all pages OR in a route, 
            depending on your layout preference */}
        <Routes>
          {/* Example route: dashboard home (no company param) */}
          <Route path="/" element={<MainDash />} />

          {/* Dynamic route for each company */}
          <Route path="/dashboard/:company" element={<MainDash />} />
        </Routes>

        <RightSide />
      </div>
    </div>
  );
}

export default App;
