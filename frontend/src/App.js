import './App.css';
import MainDash from './components/MainDash/MainDash';
import RightSide from './components/RigtSide/RightSide';
import Sidebar from './components/Sidebar';
import { Routes, Route, useParams } from 'react-router-dom';

function App() {
  // Component to dynamically handle right side content based on route params
  const RenderWithRightSide = () => {
    const { company } = useParams();

    return (
      <>
        <MainDash />
        {/* Pass the company name as a prop to RightSide */}
        {company && <RightSide company={company} />}
      </>
    );
  };

  return (
    <div className="App">
      <div className="AppGlass">
        {/* Sidebar stays visible */}
        <Sidebar />

        {/* Define routes */}
        <Routes>
          {/* Default route */}
          <Route path="/" element={<MainDash />} />

          {/* Route for company-specific dashboard */}
          <Route path="/dashboard/:company" element={<RenderWithRightSide />} />
        </Routes>
      </div>
    </div>
  );
}

export default App;
