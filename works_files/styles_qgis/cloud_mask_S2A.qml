<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="2.18.20" minimumScale="inf" maximumScale="1e+08" hasScaleBasedVisibilityFlag="0">
  <pipe>
    <rasterrenderer opacity="1" alphaBand="-1" classificationMax="8" classificationMinMaxOrigin="CumulativeCutFullExtentEstimated" band="1" classificationMin="0" type="singlebandpseudocolor">
      <rasterTransparency/>
      <rastershader>
        <colorrampshader colorRampType="INTERPOLATED" clip="0">
          <item alpha="255" value="0" label="NO_DATA" color="#000000"/>
          <item alpha="255" value="1" label="SATURATED_OR_DEFECTIVE" color="#ff0000"/>
          <item alpha="172" value="2" label="DARK_AREA_PIXELS" color="#050005"/>
          <item alpha="255" value="3" label="CLOUD_SHADOWS" color="#9b5000"/>
          <item alpha="255" value="4" label="VEGETATION" color="#00ff00"/>
          <item alpha="255" value="5" label="NOT_VEGETATED" color="#eeff00"/>
          <item alpha="255" value="6" label="WATER" color="#0f00ff"/>
          <item alpha="205" value="7" label="UNCLASSIFIED" color="#4d001e"/>
          <item alpha="120" value="8" label="CLOUD_MEDIUM_PROBABILITY" color="#1d001d"/>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast brightness="0" contrast="0"/>
    <huesaturation colorizeGreen="128" colorizeOn="0" colorizeRed="255" colorizeBlue="128" grayscaleMode="0" saturation="0" colorizeStrength="100"/>
    <rasterresampler maxOversampling="2"/>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
