Pod::Spec.new do |s|
  s.platform     = :ios, '5.0'
  s.name         = "IPOfflineQueue"
  s.version      = "0.0.4"
  s.summary      = "IPOfflineQueue is queue that stores its entries (called tasks) into SQLite (using fmdb)."
  s.homepage     = "https://github.com/shakkame/IPOfflineQueue"

  s.license      = 'MIT'
  s.author       = { "Shay Erlichmen" => "shay@shakka.me" }
  s.source       = { :git => "https://github.com/shakkame/IPOfflineQueue.git", :tag => "0.0.4" }

  s.source_files = 'Classes/IPOfflineQueue.{h,m}'

  s.dependency 'CocoaLumberjack'
  s.dependency 'FMDB'
  s.dependency 'Reachability'

  s.requires_arc = true  
end
