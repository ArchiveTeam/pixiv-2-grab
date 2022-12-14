dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false

local item_image_name = nil
local item_image_extension = nil
local pixiv_sizes = {
  ["c/48x48/img-master"] = "_square1200",
  ["c/48x48/custom-thumb"] = "_custom1200",
  ["c/128x128/img-master"] = "_square1200",
  ["c/128x128/custom-thumb"] = "_custom1200",
  ["c/250x250_80_a2/img-master"] = "_square1200",
  ["c/250x250_80_a2/custom-thumb"] = "_custom1200",
  ["c/540x540_70/img-master"] = "_master1200",
  ["img-master"] = "_master1200",
  ["img-original"] = ""
}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('queuing' , item)
    target[item] = true
  end
end

find_item = function(url)
  value = string.match(url, "^https?://www%.pixiv%.net/en/artworks/([0-9]+)$")
  type_ = "artwork"
  if not value then
    value, other = string.match(url, "^https?://i%.pximg%.net/img%-original/img/(.-/[0-9]+_p0).-(%.[a-z]+)$")
    type_ = "image"
  end
  if value then
    item_type = type_
    item_value = value
    if item_type == "image" then
      item_image_name = value
      item_image_extension = other
      item_value = item_value .. ":" .. other
    end
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[value] = true
      abortgrab = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  if string.match(url, "%?ref=twitter") then
    return false
  end

  if item_type == "artwork"
    and (
      string.match(url, "^https?://pixiv%.net/")
      or string.match(url, "^https?://www%.pixiv%.net/")
    ) then
    for s in string.gmatch(url, "([0-9]+)") do
      if ids[s] then
        return true
      end
    end
  end

  if item_type == "image"
    and (
      string.match(url, "^https?://i%.pximg%.net/")
      or string.match(url, "^https?://embed%.pixiv%.net/")
    ) then
    return true
  end

  if not string.match(url, "^https?://[^/]*pixiv%.net/")
    and not string.match(url, "^https?://i%.pximg%.net/") then
    discover_item(discovered_outlinks, url)
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"]) and not processed(url) then
    addedtolist[url] = true
    return true
  end]]

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and allowed(url_, origurl) then
      if string.match(url_, "^https?://www%.pixiv%.net/artworks/[0-9]+$") then
        table.insert(urls, { url=url_, headers={["Cookie"] = "user_language=ja"} })
      elseif string.match(url_, "^https?://i%.pximg%.net/") then
        table.insert(urls, { url=url_, headers={["Referer"] = "https://www.pixiv.net/"} })
      else
        table.insert(urls, { url=url_ })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  if string.match(url, "^https?://i%.pximg%.net/img%-original/")
    and status_code == 200 then
    local temp_start, temp_path, temp_name, temp_type, temp_extension = string.match(url, "^(https?://i%.pximg%.net/)(.-)(/img/.-/[0-9]+_p[0-9]+)(.-)(%.[a-z]+)$")
    --[[for path, format in pairs(pixiv_sizes) do
      local image_extension = temp_extension
      if format ~= "" then
        image_extension = ".jpg"
      end
      check(temp_start .. path .. temp_name .. format .. image_extension)
    end]]
    if temp_type == "" and temp_path ~= "img-original" then
      error("Odd image found.")
    end
    local image_id = string.match(temp_name, "/([0-9]+)[^/]+$")
    ids[image_id] = true
    --check("https://embed.pixiv.net/artwork.php?illust_id=" .. image_id)
    if temp_type == "" then
      local start, num = string.match(temp_name, "^(.-)([0-9]+)$")
      check(temp_start .. temp_path .. start .. tostring(tonumber(num)+1) .. temp_type .. temp_extension)
    end
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://i%.pximg%.net/")
    and not string.match(url, "^https?://embed%.pixiv%.net/") then
    html = read_file(file)
    if item_type == "artwork"
      and string.match(url, "^https?://[^/]*pixiv%.net/.*/?artworks/[0-9]+$") then
      for _, lang in pairs({"ja", "en"}) do
        check("https://www.pixiv.net/ajax/illust/" .. item_value .. "/recommend/init?limit=30&lang=" .. lang)
        check("https://www.pixiv.net/ajax/illust/" .. item_value .. "/pages?lang=" .. lang)
        check("https://www.pixiv.net/ajax/illusts/comments/roots?illust_id=" .. item_value .. "&offset=0&limit=3&lang=" .. lang)
      end
      check("https://www.pixiv.net/artworks/" .. item_value)
      local global_data = JSON:decode(string.match(html, '<meta%s+name="global%-data"[^>]*%s+content=\'({.-})\''))
      local preload_data = JSON:decode(string.match(html, '<meta%s+name="preload%-data"[^>]+%s+content=\'({.-})\''))
      local image_item = nil
      local image_extension = nil
      for name, url in pairs(preload_data["illust"][item_value]["urls"]) do
        print("Found", name, url)
        local temp_size, temp_name, temp_type, temp_extension = string.match(url, "^https?://i%.pximg%.net/(.-)/img/(.-" .. item_value .. "_p0)(.-)(%.[a-z]+)$")
        if not temp_name then
          print("No image name found in URL " .. url .. ".")
          abort_item()
          return {}
        end
        if pixiv_sizes[temp_size] ~= temp_type then
          print("Wrong image type found " .. temp_type .. ".")
          abort_item()
          return {}
        end
        local temp_item = "image:" .. temp_name
        if image_item and temp_item ~= image_item then
          print("Inconsistent image name " .. temp_item .. ".")
          abort_item()
          return {}
        end
        image_item = temp_item
        if temp_type == "" then
          image_extension = temp_extension
        end
      end
      if not image_item or not image_extension then
        error("No image found.")
      end
      image_item = image_item .. ":" .. image_extension
      discover_item(discovered_items, image_item)
    end
    if string.match(url, "/ajax/illusts/comments/roots") then
      local json = JSON:decode(html)
      if json["body"]["hasNext"] then
        local count = 0
        for _ in pairs(json["body"]["comments"]) do
          count = count + 1
        end
        count = count + tonumber(string.match(url, "offset=([0-9]+)"))
        local newurl = url
        newurl = string.gsub(newurl, "offset=[0-9]+", "offset=" .. tostring(count))
        newurl = string.gsub(newurl, "limit=[0-9]+", "limit=50")
        check(newurl)
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  find_item(url["url"])
  if not item_name then
    error("No item name found.")
  end
  if http_stat["len"] == 0 and http_stat["statcode"] == 200 then
    io.stdout:write("Zero-length content found for URL " .. url["url"] .. " on status code 200.")
    io.stdout:flush()
    os.execute("sleep 1800")
    error("zero length")
  end
  if string.match(url["url"], "^https?://i%.pximg%.net/")
    or string.match(url["url"], "^https?://embed%.pixiv%.net/") then
    if http_stat["statcode"] == 200 then
      return true
    elseif http_stat["statcode"] == 404 then
      if string.match(url["url"], "p0")
        and not string.match(url["url"], "_custom1200") then
        error("Got status code 404 on p0 image.")
      end
      return false
    end
  end
  if http_stat["statcode"] ~= 200
    and not string.match(url["url"], "/pages%?lang=") then
    if not string.match(url["url"], "^https?://www%.pixiv%.net/en/artworks/[0-9]+$") then
      error("Odd status code " .. tostring(http_stat["statcode"]) .. " for URL " .. url["url"] .. ".")
    end
    io.stdout:write("Not writing bad response to WARC.\n")
    io.stdout:flush()
    return false
  end
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  find_item(url["url"])

  --[[if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end]]

  if status_code == 404
    and string.match(url["url"], "^https?://www%.pixiv%.net/en/artworks/[0-9]+$") then
    abort_item()
  end
  
  if status_code == 200 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or
    (status_code ~= 200 and status_code ~= 404) then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 0
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["pixiv2-5i9uyf6jcx43j49"] = discovered_items,
    ["urls-2t6fmgh695djpc5"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

