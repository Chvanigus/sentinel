<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis hasScaleBasedVisibilityFlag="0" minScale="1e+08" styleCategories="AllStyleCategories" version="3.18.1-Zürich" maxScale="0">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
    <Private>0</Private>
  </flags>
  <temporal mode="0" enabled="0" fetchMode="0">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <customproperties>
    <property key="WMSBackgroundLayer" value="false"/>
    <property key="WMSPublishDataSourceUrl" value="false"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="identify/format" value="Value"/>
  </customproperties>
  <pipe>
    <provider>
      <resampling enabled="false" maxOversampling="2" zoomedInResamplingMethod="nearestNeighbour" zoomedOutResamplingMethod="nearestNeighbour"/>
    </provider>
    <rasterrenderer opacity="1" nodataColor="" band="1" type="singlebandpseudocolor" classificationMin="0" classificationMax="11" alphaBand="-1">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader labelPrecision="0" classificationMode="1" minimumValue="0" clip="0" maximumValue="11" colorRampType="INTERPOLATED">
          <colorramp name="[source]" type="gradient">
            <Option type="Map">
              <Option name="color1" value="0,0,0,255" type="QString"/>
              <Option name="color2" value="255,0,255,255" type="QString"/>
              <Option name="discrete" value="0" type="QString"/>
              <Option name="rampType" value="gradient" type="QString"/>
              <Option name="stops" value="0.0909091;255,0,12,255:0.181818;61,61,61,255:0.272727;134,65,0,255:0.363636;30,255,1,255:0.454545;226,255,0,255:0.545455;17,0,255,255:0.636364;101,101,101,255:0.727273;180,180,180,255:0.818182;117,117,117,255:0.909091;83,230,255,255" type="QString"/>
            </Option>
            <prop k="color1" v="0,0,0,255"/>
            <prop k="color2" v="255,0,255,255"/>
            <prop k="discrete" v="0"/>
            <prop k="rampType" v="gradient"/>
            <prop k="stops" v="0.0909091;255,0,12,255:0.181818;61,61,61,255:0.272727;134,65,0,255:0.363636;30,255,1,255:0.454545;226,255,0,255:0.545455;17,0,255,255:0.636364;101,101,101,255:0.727273;180,180,180,255:0.818182;117,117,117,255:0.909091;83,230,255,255"/>
          </colorramp>
          <item alpha="255" value="0" color="#000000" label="Нет данных"/>
          <item alpha="255" value="1" color="#ff000c" label="&#xa;Насыщенный или дефектный"/>
          <item alpha="255" value="2" color="#3d3d3d" label="Зона тёмных пикселей"/>
          <item alpha="255" value="3" color="#864100" label="Тень от облака"/>
          <item alpha="255" value="4" color="#1eff01" label="&#xa;Растительность"/>
          <item alpha="255" value="5" color="#e2ff00" label="Не растительность"/>
          <item alpha="255" value="6" color="#1100ff" label="Вода"/>
          <item alpha="255" value="7" color="#656565" label="Неклассифицировано"/>
          <item alpha="255" value="8" color="#b4b4b4" label="Cредняя вероятность облачности"/>
          <item alpha="255" value="9" color="#757575" label="Высокая вероятность облачности"/>
          <item alpha="255" value="10" color="#53e6ff" label="Перистые облака"/>
          <item alpha="255" value="11" color="#ff00ff" label="Снег"/>
          <rampLegendSettings maximumLabel="" direction="0" useContinuousLegend="1" suffix="" minimumLabel="" prefix="" orientation="2">
            <numericFormat id="basic">
              <Option type="Map">
                <Option name="decimal_separator" value="" type="QChar"/>
                <Option name="decimals" value="6" type="int"/>
                <Option name="rounding_type" value="0" type="int"/>
                <Option name="show_plus" value="false" type="bool"/>
                <Option name="show_thousand_separator" value="true" type="bool"/>
                <Option name="show_trailing_zeros" value="false" type="bool"/>
                <Option name="thousand_separator" value="" type="QChar"/>
              </Option>
            </numericFormat>
          </rampLegendSettings>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast contrast="0" gamma="1" brightness="0"/>
    <huesaturation saturation="0" colorizeStrength="100" colorizeRed="255" grayscaleMode="0" colorizeOn="0" colorizeGreen="128" colorizeBlue="128"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
