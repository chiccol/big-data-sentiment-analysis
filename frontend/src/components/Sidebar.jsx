import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom"; // <-- for navigation
import "./Sidebar.css";
import Logo from "../imgs/logo.png";
import { UilSignOutAlt, UilBars, UilEstate } from "@iconscout/react-unicons";
import { getCompaniesData } from "../Data/Data";
import { motion } from "framer-motion";

const Sidebar = () => {
  const [selected, setSelected] = useState(0);
  const [expanded, setExpaned] = useState(true);
  const [companies, setCompanies] = useState([]);
  const navigate = useNavigate();

  // Fetch companies on component mount
  useEffect(() => {
    const fetchCompanies = async () => {
      const data = await getCompaniesData();
      setCompanies(data);
    };
    fetchCompanies();
  }, []);

  const sidebarVariants = {
    true: {
      left: "0",
    },
    false: {
      left: "-60%",
    },
  };

  const handleMenuClick = (index, companyName) => {
    setSelected(index);
    // Navigate to /dashboard/{companyName}
    navigate(`/dashboard/${companyName}`);
  };

  return (
    <>
      <div
        className="bars"
        style={expanded ? { left: "60%" } : { left: "5%" }}
        onClick={() => setExpaned(!expanded)}
      >
        <UilBars />
      </div>

      <motion.div
        className="sidebar"
        variants={sidebarVariants}
        animate={window.innerWidth <= 768 ? `${expanded}` : ""}
      >
        {/* logo */}
        <div className="logo">
          <img src={Logo} alt="logo" />
          <span>
            Sh<span>o</span>ps
          </span>
        </div>

        <div className="menu">
          {/* Dynamically render list of companies */}
          {companies.map((company, index) => (
            <div
              className={selected === index ? "menuItem active" : "menuItem"}
              key={index}
              onClick={() => handleMenuClick(index, company)}
            >
              {/* Use a placeholder icon to keep design consistent */}
              <UilEstate />
              <span>{company}</span>
            </div>
          ))}

          {/* signoutIcon */}
          <div className="menuItem">
            <UilSignOutAlt />
          </div>
        </div>
      </motion.div>
    </>
  );
};

export default Sidebar;
