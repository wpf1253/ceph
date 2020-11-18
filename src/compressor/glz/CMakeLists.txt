# glz 

set(glz_sources
  CompressionPluginGLZ.cc
)

add_library(ceph_glz SHARED ${glz_sources})
target_link_libraries(ceph_glz PRIVATE glz)
set_target_properties(ceph_glz PROPERTIES
  VERSION 2.0.0
  SOVERSION 2
  INSTALL_RPATH "")
install(TARGETS ceph_glz DESTINATION ${compressor_plugin_dir})
