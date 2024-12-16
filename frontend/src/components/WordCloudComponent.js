import React, { useEffect, useRef } from 'react';
import cloud from 'd3-cloud';
import * as d3 from 'd3';

const WordCloud = ({ words = [] }) => {
  const svgRef = useRef();

  useEffect(() => {
    if (!words || words.length === 0) {
      console.warn("No words provided to WordCloud component.");
      return; // Exit early if words are undefined or empty
    }

    const layout = cloud()
      .size([500, 500])
      .words(words.map(d => ({ text: d.text, size: d.value })))
      .padding(5)
      .rotate(() => (Math.random() > 0.5 ? 90 : 0))
      .font('Impact')
      .fontSize(d => d.size)
      .on('end', draw);

    layout.start();

    function draw(words) {
      const svg = d3.select(svgRef.current);
      svg.selectAll('*').remove(); // Clear previous word cloud

      svg
        .attr('width', layout.size()[0])
        .attr('height', layout.size()[1])
        .append('g')
        .attr(
          'transform',
          `translate(${layout.size()[0] / 2},${layout.size()[1] / 2})`
        )
        .selectAll('text')
        .data(words)
        .enter()
        .append('text')
        .style('font-size', d => `${d.size}px`)
        .style('fill', () => `hsl(${Math.random() * 360}, 100%, 50%)`)
        .attr('text-anchor', 'middle')
        .attr('transform', d => `translate(${d.x},${d.y})rotate(${d.rotate})`)
        .text(d => d.text);
    }
  }, [words]);

  return <svg ref={svgRef}></svg>;
};

export default WordCloud;
